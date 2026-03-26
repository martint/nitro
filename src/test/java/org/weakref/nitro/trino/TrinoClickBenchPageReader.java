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
package org.weakref.nitro.trino;

import io.airlift.units.DataSize;
import io.trino.memory.context.AggregatedMemoryContext;
import io.trino.parquet.AbstractParquetDataSource;
import io.trino.parquet.Column;
import io.trino.parquet.ParquetDataSourceId;
import io.trino.parquet.ParquetReaderOptions;
import io.trino.parquet.ParquetTypeUtils;
import io.trino.parquet.PrimitiveField;
import io.trino.parquet.metadata.ParquetMetadata;
import io.trino.parquet.metadata.PrunedBlockMetadata;
import io.trino.parquet.reader.MetadataReader;
import io.trino.parquet.reader.ParquetReader;
import io.trino.parquet.reader.RowGroupInfo;
import io.trino.spi.Page;
import io.trino.spi.connector.SourcePage;
import org.apache.parquet.column.ColumnDescriptor;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.io.LocalInputFile;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;
import org.joda.time.DateTimeZone;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.RandomAccessFile;
import java.io.UncheckedIOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkArgument;
import static io.airlift.units.DataSize.Unit.MEGABYTE;
import static java.util.Objects.requireNonNull;
import static org.apache.parquet.schema.LogicalTypeAnnotation.stringType;
import static org.apache.parquet.schema.Type.Repetition.REQUIRED;

final class TrinoClickBenchPageReader
        implements AutoCloseable
{
    private static final String MAX_BATCH_ROWS_PROPERTY = "nitro.trino.scan.maxBatchRows";
    private static final int DEFAULT_MAX_BATCH_ROWS = 10_000;
    private static final int MAX_BATCH_ROWS = Integer.getInteger(MAX_BATCH_ROWS_PROPERTY, DEFAULT_MAX_BATCH_ROWS);
    private static final DataSize MAX_READ_BLOCK_SIZE = DataSize.of(2, MEGABYTE);
    private static final DataSize MAX_MERGE_DISTANCE = DataSize.of(1, MEGABYTE);
    private static final DataSize MAX_BUFFER_SIZE = DataSize.of(2, MEGABYTE);
    private static final DataSize MAX_PAGE_READ_SIZE = DataSize.of(2, MEGABYTE);
    private static final String CLICKBENCH_HITS_PATH_PROPERTY = "nitro.clickbench.hits.path";

    private final List<Path> files;
    private final List<String> columnNames;

    private int fileIndex;
    private SingleFileReader currentFile;

    TrinoClickBenchPageReader(Path input, List<String> columnNames)
    {
        this(resolveFiles(input), columnNames);
    }

    TrinoClickBenchPageReader(List<Path> files, List<String> columnNames)
    {
        requireNonNull(files, "files is null");
        requireNonNull(columnNames, "columnNames is null");
        checkArgument(!files.isEmpty(), "files is empty");
        this.files = List.copyOf(files);
        this.columnNames = List.copyOf(columnNames);
    }

    public static Path requiredActualHitsPath()
    {
        String configuredPath = System.getProperty(CLICKBENCH_HITS_PATH_PROPERTY);
        if (configuredPath != null && !configuredPath.isBlank()) {
            return Path.of(configuredPath);
        }

        Path defaultPath = Path.of(System.getProperty("user.home"), "tmp", "clickbench");
        if (java.nio.file.Files.exists(defaultPath)) {
            return defaultPath;
        }

        throw new IllegalStateException("Set -D" + CLICKBENCH_HITS_PATH_PROPERTY + "=/path/to/clickbench, or place the split parquet files at ~/tmp/clickbench");
    }

    public static List<Path> resolveFiles(Path input)
    {
        requireNonNull(input, "input is null");
        try {
            if (java.nio.file.Files.isDirectory(input)) {
                try (java.util.stream.Stream<Path> stream = java.nio.file.Files.list(input)) {
                    List<Path> files = stream
                            .filter(path -> path.getFileName().toString().endsWith(".parquet"))
                            .sorted(Comparator.comparing(path -> path.getFileName().toString()))
                            .toList();
                    checkArgument(!files.isEmpty(), "No parquet files found in %s", input);
                    return files;
                }
            }
            checkArgument(java.nio.file.Files.exists(input), "Input does not exist: %s", input);
            return List.of(input);
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to list parquet files for " + input, exception);
        }
    }

    public static List<String> allColumns(Path input)
    {
        Path file = resolveFiles(input).getFirst();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            return reader.getFileMetaData().getSchema().getFields().stream()
                    .map(Type::getName)
                    .toList();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench schema from " + file, exception);
        }
    }

    public static List<io.trino.spi.type.Type> columnTypes(Path input, List<String> columnNames)
    {
        Path file = resolveFiles(input).getFirst();
        try (ParquetFileReader reader = ParquetFileReader.open(new LocalInputFile(file))) {
            MessageType schema = reader.getFileMetaData().getSchema();
            return columnNames.stream()
                    .map(name -> parquetType(findField(schema, name).asPrimitiveType()))
                    .toList();
        }
        catch (IOException exception) {
            throw new UncheckedIOException("Unable to inspect ClickBench schema from " + file, exception);
        }
    }

    public boolean hasNext()
    {
        advanceIfNecessary();
        return currentFile != null && currentFile.hasNext();
    }

    public Page nextPage()
    {
        if (!hasNext()) {
            throw new IllegalStateException("No more ClickBench pages");
        }
        return currentFile.nextPage();
    }

    @Override
    public void close()
    {
        if (currentFile != null) {
            currentFile.close();
            currentFile = null;
        }
    }

    private void advanceIfNecessary()
    {
        while ((currentFile == null || !currentFile.hasNext()) && fileIndex < files.size()) {
            if (currentFile != null) {
                currentFile.close();
            }
            currentFile = new SingleFileReader(files.get(fileIndex++), columnNames);
        }
    }

    private static final class SingleFileReader
            implements AutoCloseable
    {
        private final ParquetReader reader;

        private SourcePage nextPage;
        private boolean exhausted;

        private SingleFileReader(Path file, List<String> columnNames)
        {
            requireNonNull(file, "file is null");
            requireNonNull(columnNames, "columnNames is null");

            try {
                ParquetReaderOptions options = ParquetReaderOptions.builder()
                        .withMaxReadBlockSize(MAX_READ_BLOCK_SIZE)
                        .withMaxReadBlockRowCount(MAX_BATCH_ROWS)
                        .withMaxMergeDistance(MAX_MERGE_DISTANCE)
                        .withMaxBufferSize(MAX_BUFFER_SIZE)
                        .withMaxPageReadSize(MAX_PAGE_READ_SIZE)
                        .withVectorizedDecodingEnabled(false)
                        .build();

                FileParquetDataSource dataSource = new FileParquetDataSource(file.toFile(), options);
                ParquetMetadata metadata = MetadataReader.readFooter(dataSource, Optional.empty());
                MessageType schema = metadata.getFileMetaData().getSchema();
                MessageType requestedSchema = new MessageType(schema.getName(), columnNames.stream()
                        .map(name -> findField(schema, name))
                        .toList());
                Map<List<String>, ColumnDescriptor> descriptorsByPath = ParquetTypeUtils.getDescriptors(schema, requestedSchema);
                List<Column> columns = resolveColumns(schema, columnNames, descriptorsByPath);
                List<RowGroupInfo> rowGroups = metadata.getBlocks().stream()
                        .map(block -> createRowGroupInfo(block, dataSource.getId(), descriptorsByPath))
                        .toList();

                reader = new ParquetReader(
                        Optional.ofNullable(metadata.getFileMetaData().getCreatedBy()),
                        columns,
                        false,
                        rowGroups,
                        dataSource,
                        DateTimeZone.UTC,
                        AggregatedMemoryContext.newSimpleAggregatedMemoryContext(),
                        options,
                        exception -> new RuntimeException("Unable to read Trino Parquet page from " + file, exception),
                        Optional.empty(),
                        Optional.empty(),
                        metadata.getDecryptionContext());
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to open Trino Parquet file: " + file, exception);
            }
        }

        public boolean hasNext()
        {
            if (!exhausted && nextPage == null) {
                nextPage = loadNextPage();
                exhausted = nextPage == null;
            }
            return nextPage != null;
        }

        public Page nextPage()
        {
            if (!hasNext()) {
                throw new IllegalStateException("No more Trino Parquet pages");
            }
            SourcePage page = nextPage;
            nextPage = null;
            return page.getPage();
        }

        @Override
        public void close()
        {
            try {
                reader.close();
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to close Trino Parquet reader", exception);
            }
            finally {
                nextPage = null;
                exhausted = true;
            }
        }

        private SourcePage loadNextPage()
        {
            try {
                while (true) {
                    SourcePage page = reader.nextPage();
                    if (page == null || page.getPositionCount() > 0) {
                        return page;
                    }
                }
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to read Trino Parquet page", exception);
            }
        }

        private static List<Column> resolveColumns(MessageType schema, List<String> columnNames, Map<List<String>, ColumnDescriptor> descriptorsByPath)
        {
            List<Column> resolvedColumns = new ArrayList<>(columnNames.size());
            for (int columnIndex = 0; columnIndex < columnNames.size(); columnIndex++) {
                String name = columnNames.get(columnIndex);
                Type field = findField(schema, name);
                checkArgument(field.isPrimitive(), "Only flat primitive Parquet columns are supported by Trino comparison harness: %s", name);

                PrimitiveType primitive = field.asPrimitiveType();
                ColumnDescriptor descriptor = requireNonNull(descriptorsByPath.get(List.of(field.getName())), "descriptor is null for " + name);
                PrimitiveField primitiveField = new PrimitiveField(parquetType(primitive), field.getRepetition() == REQUIRED, descriptor, columnIndex);
                resolvedColumns.add(new Column(name, primitiveField));
            }
            return resolvedColumns;
        }

        private static io.trino.spi.type.Type parquetType(PrimitiveType primitive)
        {
            return switch (primitive.getPrimitiveTypeName()) {
                case INT32 -> io.trino.spi.type.IntegerType.INTEGER;
                case INT64 -> io.trino.spi.type.BigintType.BIGINT;
                case BOOLEAN -> io.trino.spi.type.BooleanType.BOOLEAN;
                case BINARY, FIXED_LEN_BYTE_ARRAY -> primitive.getLogicalTypeAnnotation() != null && primitive.getLogicalTypeAnnotation().equals(stringType())
                        ? io.trino.spi.type.VarcharType.VARCHAR
                        : io.trino.spi.type.VarbinaryType.VARBINARY;
                default -> throw new IllegalArgumentException("Unsupported Trino Parquet primitive type: " + primitive.getPrimitiveTypeName());
            };
        }

        private static RowGroupInfo createRowGroupInfo(io.trino.parquet.metadata.BlockMetadata block, ParquetDataSourceId dataSourceId, Map<List<String>, ColumnDescriptor> descriptorsByPath)
        {
            try {
                return new RowGroupInfo(
                        PrunedBlockMetadata.createPrunedColumnsMetadata(block, dataSourceId, descriptorsByPath),
                        block.fileRowCountOffset(),
                        Optional.empty());
            }
            catch (IOException exception) {
                throw new UncheckedIOException("Unable to prune Trino row-group metadata", exception);
            }
        }
    }

    private static Type findField(MessageType schema, String name)
    {
        for (Type field : schema.getFields()) {
            if (field.getName().equalsIgnoreCase(name)) {
                return field;
            }
        }
        throw new IllegalArgumentException("Unknown Parquet column: " + name);
    }

    private static io.trino.spi.type.Type parquetType(PrimitiveType primitive)
    {
        return switch (primitive.getPrimitiveTypeName()) {
            case INT32 -> io.trino.spi.type.IntegerType.INTEGER;
            case INT64 -> io.trino.spi.type.BigintType.BIGINT;
            case BOOLEAN -> io.trino.spi.type.BooleanType.BOOLEAN;
            case BINARY, FIXED_LEN_BYTE_ARRAY -> primitive.getLogicalTypeAnnotation() != null && primitive.getLogicalTypeAnnotation().equals(stringType())
                    ? io.trino.spi.type.VarcharType.VARCHAR
                    : io.trino.spi.type.VarbinaryType.VARBINARY;
            default -> throw new IllegalArgumentException("Unsupported Trino Parquet primitive type: " + primitive.getPrimitiveTypeName());
        };
    }

    private static final class FileParquetDataSource
            extends AbstractParquetDataSource
    {
        private final RandomAccessFile input;

        private FileParquetDataSource(File file, ParquetReaderOptions options)
                throws FileNotFoundException
        {
            super(new ParquetDataSourceId(file.toString()), file.length(), options);
            this.input = new RandomAccessFile(file, "r");
        }

        @Override
        protected void readInternal(long position, byte[] buffer, int bufferOffset, int bufferLength)
                throws IOException
        {
            input.seek(position);
            input.readFully(buffer, bufferOffset, bufferLength);
        }

        @Override
        public void close()
                throws IOException
        {
            input.close();
        }
    }
}
