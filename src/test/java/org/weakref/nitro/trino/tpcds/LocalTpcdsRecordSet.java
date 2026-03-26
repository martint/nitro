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
package org.weakref.nitro.trino.tpcds;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import io.trino.spi.connector.RecordCursor;
import io.trino.spi.connector.RecordSet;
import io.trino.spi.type.DecimalParseResult;
import io.trino.spi.type.DecimalType;
import io.trino.spi.type.Decimals;
import io.trino.spi.type.Type;
import io.trino.tpcds.Results;
import io.trino.tpcds.column.Column;
import io.trino.tpcds.column.ColumnType;

import java.time.LocalDate;
import java.time.LocalTime;
import java.util.Iterator;
import java.util.List;

import static com.google.common.base.Preconditions.checkState;
import static io.trino.spi.type.Chars.trimTrailingSpaces;
import static io.trino.spi.type.Decimals.rescale;
import static io.trino.spi.type.Timestamps.PICOSECONDS_PER_MILLISECOND;
import static java.lang.Double.parseDouble;
import static java.lang.Integer.parseInt;
import static java.lang.Long.parseLong;
import static java.util.Objects.requireNonNull;

public final class LocalTpcdsRecordSet
        implements RecordSet
{
    private final Results results;
    private final List<Column> columns;
    private final List<Type> columnTypes;

    public LocalTpcdsRecordSet(Results results, List<Column> columns)
    {
        this.results = requireNonNull(results, "results is null");
        this.columns = ImmutableList.copyOf(columns);
        this.columnTypes = this.columns.stream()
                .map(Column::getType)
                .map(LocalTpcdsMetadata::trinoType)
                .toList();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new Cursor(results.iterator(), columns, columnTypes);
    }

    private static final class Cursor
            implements RecordCursor
    {
        private final Iterator<List<List<String>>> rows;
        private final List<Column> columns;
        private final List<Type> columnTypes;
        private List<String> row;
        private boolean closed;

        private Cursor(Iterator<List<List<String>>> rows, List<Column> columns, List<Type> columnTypes)
        {
            this.rows = requireNonNull(rows, "rows is null");
            this.columns = requireNonNull(columns, "columns is null");
            this.columnTypes = requireNonNull(columnTypes, "columnTypes is null");
        }

        @Override
        public long getCompletedBytes()
        {
            return 0;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            return columnTypes.get(field);
        }

        @Override
        public boolean advanceNextPosition()
        {
            if (closed || !rows.hasNext()) {
                closed = true;
                row = null;
                return false;
            }
            row = rows.next().getFirst();
            return true;
        }

        @Override
        public boolean getBoolean(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public long getLong(int field)
        {
            checkState(row != null, "No current row");
            Column column = columns.get(field);
            String value = row.get(column.getPosition());
            return switch (column.getType().getBase()) {
                case DATE -> LocalDate.parse(value).toEpochDay();
                case TIME -> {
                    LocalTime time = LocalTime.parse(value);
                    long milliseconds = (long) time.toSecondOfDay() * 1000 + time.getNano() / 1_000_000;
                    yield milliseconds * PICOSECONDS_PER_MILLISECOND;
                }
                case INTEGER -> parseInt(value);
                case DECIMAL -> {
                    DecimalParseResult parsed = Decimals.parse(value);
                    yield rescale((Long) parsed.getObject(), parsed.getType().getScale(), ((DecimalType) columnTypes.get(field)).getScale());
                }
                default -> parseLong(value);
            };
        }

        @Override
        public double getDouble(int field)
        {
            checkState(row != null, "No current row");
            return parseDouble(row.get(columns.get(field).getPosition()));
        }

        @Override
        public Slice getSlice(int field)
        {
            checkState(row != null, "No current row");
            Column column = columns.get(field);
            String value = row.get(column.getPosition());
            if (column.getType().getBase() == ColumnType.Base.DECIMAL) {
                return (Slice) Decimals.parse(value).getObject();
            }
            Slice slice = Slices.utf8Slice(value);
            if (column.getType().getBase() == ColumnType.Base.CHAR) {
                return trimTrailingSpaces(slice);
            }
            return slice;
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkState(row != null, "No current row");
            return row.get(columns.get(field).getPosition()) == null;
        }

        @Override
        public void close()
        {
            row = null;
            closed = true;
        }
    }
}
