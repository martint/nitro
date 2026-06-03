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

import org.weakref.nitro.data.Allocator;
import org.weakref.nitro.data.BinaryVector;
import org.weakref.nitro.data.BooleanVector;
import org.weakref.nitro.data.F64Vector;
import org.weakref.nitro.data.I32Vector;
import org.weakref.nitro.data.I64Vector;
import org.weakref.nitro.data.Mask;
import org.weakref.nitro.data.Row;

import java.nio.charset.StandardCharsets;
import java.util.List;

public class ConstantTableOperator
        implements Operator
{
    private static final Allocator.Context ALLOCATION_CONTEXT = new Allocator.Context("ConstantTableOperator");
    private final Allocator allocator;

    private final Streams[] columns;
    private final int count;
    private boolean done;

    public ConstantTableOperator(Allocator allocator, int columnCount, List<Row> rows)
    {
        this.allocator = allocator;
        columns = new Streams[columnCount];
        for (int column = 0; column < columns.length; column++) {
            columns[column] = buildColumn(rows, column);
        }

        this.count = rows.size();
    }

    @Override
    public int outputCount()
    {
        return columns.length;
    }

    @Override
    public boolean hasNext()
    {
        return !done;
    }

    @Override
    public Batch next()
    {
        done = true;
        Output[] outputs = new Output[outputCount()];
        for (int outputIndex = 0; outputIndex < outputs.length; outputIndex++) {
            Streams streams = columns[outputIndex];
            outputs[outputIndex] = new Output(streams.asMap().keySet(), streams::get, (stream, vector) -> allocator.transfer(ALLOCATION_CONTEXT, vector));
        }
        return new Batch(allocator.allocateAllMask(ALLOCATION_CONTEXT, count), takenMask -> allocator.transfer(ALLOCATION_CONTEXT, takenMask), outputs);
    }

    @Override
    public void constrain(Mask mask)
    {
    }

    @Override
    public boolean supportsRetainedBatches()
    {
        return false;
    }

    @Override
    public boolean supportsConstrainedReborrow()
    {
        // The in-memory rows stay valid for the operator's lifetime and are re-supplied on demand,
        // indexed by absolute source position, so a constrained re-borrow yields the requested rows.
        return true;
    }

    @Override
    public void close()
    {
        allocator.release(ALLOCATION_CONTEXT);
    }

    private Streams buildColumn(List<Row> rows, int column)
    {
        ColumnKind kind = inferColumnKind(rows, column);
        return switch (kind) {
            case I32 -> buildI32Column(rows, column);
            case I64 -> buildI64Column(rows, column);
            case BOOLEAN -> buildBooleanColumn(rows, column);
            case F64 -> buildDoubleColumn(rows, column);
            case UTF8 -> buildUtf8Column(rows, column);
            case BINARY -> buildBinaryColumn(rows, column);
        };
    }

    private Streams buildI32Column(List<Row> rows, int column)
    {
        I32Vector values = allocator.allocate(ALLOCATION_CONTEXT, I32Vector.class, rows.size(), I32Vector::new);
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                nulls.values()[position] = true;
                continue;
            }
            values.values()[position] = ((Number) value).intValue();
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private Streams buildI64Column(List<Row> rows, int column)
    {
        I64Vector values = allocator.allocate(ALLOCATION_CONTEXT, I64Vector.class, rows.size(), I64Vector::new);
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                nulls.values()[position] = true;
                continue;
            }
            values.values()[position] = ((Number) value).longValue();
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private Streams buildBooleanColumn(List<Row> rows, int column)
    {
        BooleanVector values = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                nulls.values()[position] = true;
                continue;
            }
            values.values()[position] = (Boolean) value;
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private Streams buildDoubleColumn(List<Row> rows, int column)
    {
        F64Vector values = allocator.allocate(ALLOCATION_CONTEXT, F64Vector.class, rows.size(), F64Vector::new);
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                nulls.values()[position] = true;
                continue;
            }
            values.values()[position] = ((Number) value).doubleValue();
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private Streams buildUtf8Column(List<Row> rows, int column)
    {
        int byteCapacity = 0;
        boolean asciiOnly = true;
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                continue;
            }
            byte[] bytes = ((String) value).getBytes(StandardCharsets.UTF_8);
            byteCapacity += bytes.length;
            asciiOnly &= bytes.length == ((String) value).length();
        }

        BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, rows.size(), byteCapacity);
        values.addTrait(org.weakref.nitro.data.Utf8Traits.UTF8_STRING);
        if (asciiOnly) {
            values.addTrait(org.weakref.nitro.data.Utf8Traits.ASCII_ONLY);
        }
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                nulls.values()[position] = true;
                values.setNull(position);
                continue;
            }
            values.setBytes(position, ((String) value).getBytes(StandardCharsets.UTF_8));
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private Streams buildBinaryColumn(List<Row> rows, int column)
    {
        int byteCapacity = 0;
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value != null) {
                byteCapacity += ((byte[]) value).length;
            }
        }

        BinaryVector values = BinaryVector.allocate(allocator, ALLOCATION_CONTEXT, rows.size(), byteCapacity);
        BooleanVector nulls = allocator.allocate(ALLOCATION_CONTEXT, BooleanVector.class, rows.size(), BooleanVector::new);
        for (int position = 0; position < rows.size(); position++) {
            Object value = value(rows.get(position), column);
            if (value == null) {
                nulls.values()[position] = true;
                values.setNull(position);
                continue;
            }
            values.setBytes(position, (byte[]) value);
        }
        return Streams.ofValuesAndNulls(values, nulls);
    }

    private static ColumnKind inferColumnKind(List<Row> rows, int column)
    {
        ColumnKind kind = null;
        for (Row row : rows) {
            Object value = value(row, column);
            if (value == null) {
                continue;
            }

            ColumnKind candidate = columnKind(value);
            if (kind == null) {
                kind = candidate;
                continue;
            }
            if (kind != candidate) {
                throw new IllegalArgumentException("Mixed column types are not supported in ConstantTableOperator");
            }
        }
        return kind == null ? ColumnKind.I64 : kind;
    }

    private static ColumnKind columnKind(Object value)
    {
        return switch (value) {
            case Integer _, Short _, Byte _ -> ColumnKind.I32;
            case Long _ -> ColumnKind.I64;
            case Boolean _ -> ColumnKind.BOOLEAN;
            case Double _, Float _ -> ColumnKind.F64;
            case String _ -> ColumnKind.UTF8;
            case byte[] _ -> ColumnKind.BINARY;
            default -> throw new IllegalArgumentException("Unsupported ConstantTableOperator value type: " + value.getClass().getSimpleName());
        };
    }

    private static Object value(Row row, int column)
    {
        Object[] values = row.values();
        if (column >= values.length) {
            throw new IllegalArgumentException("Row has fewer columns than expected");
        }
        return values[column];
    }

    private enum ColumnKind
    {
        I32,
        I64,
        BOOLEAN,
        F64,
        UTF8,
        BINARY,
    }
}
