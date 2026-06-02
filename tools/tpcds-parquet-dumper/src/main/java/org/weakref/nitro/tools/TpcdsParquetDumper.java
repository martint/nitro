package org.weakref.nitro.tools;

import io.trino.Session;
import io.trino.plugin.hive.HivePlugin;
import io.trino.plugin.tpcds.TpcdsPlugin;
import io.trino.testing.MaterializedResult;
import io.trino.testing.QueryRunner;
import io.trino.testing.StandaloneQueryRunner;

import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.Map;

import static io.trino.testing.TestingSession.testSessionBuilder;

public final class TpcdsParquetDumper
{
    private TpcdsParquetDumper() {}

    public static void main(String[] args)
            throws Exception
    {
        Arguments arguments = Arguments.parse(args);
        Files.createDirectories(arguments.outputRoot());

        Session session = testSessionBuilder()
                .setCatalog("hive")
                .setSchema("default")
                .setCatalogSessionProperty("hive", "compression_codec", arguments.parquetCompression())
                .build();

        try (QueryRunner queryRunner = new StandaloneQueryRunner(session)) {
            queryRunner.installPlugin(new TpcdsPlugin());
            queryRunner.createCatalog("tpcds", "tpcds");

            queryRunner.installPlugin(new HivePlugin());
            queryRunner.createCatalog("hive", "hive", Map.of(
                    "hive.metastore", "file",
                    "hive.metastore.catalog.dir", arguments.outputRoot().toAbsolutePath().toString(),
                    "fs.hadoop.enabled", "true",
                    "hive.security", "allow-all"));
            String targetSchema = arguments.targetSchema();
            execute(queryRunner, "CREATE SCHEMA IF NOT EXISTS hive." + quotedIdentifier(targetSchema));

            List<String> tables = arguments.tables().isEmpty() ? allSourceTables(queryRunner, arguments.sourceSchema()) : arguments.tables();
            System.out.printf("Dumping %d TPC-DS tables from tpcds.%s to hive.%s under %s%n",
                    tables.size(),
                    arguments.sourceSchema(),
                    targetSchema,
                    arguments.outputRoot().toAbsolutePath());

            for (String table : tables) {
                dumpTable(queryRunner, arguments, table);
            }
        }
    }

    private static void dumpTable(QueryRunner queryRunner, Arguments arguments, String table)
    {
        String quotedTargetSchema = quotedIdentifier(arguments.targetSchema());
        String quotedTable = quotedIdentifier(table);
        String targetName = "hive." + quotedTargetSchema + "." + quotedTable;
        if (arguments.overwrite()) {
            execute(queryRunner, "DROP TABLE IF EXISTS " + targetName);
        }
        else if (tableExists(queryRunner, arguments.targetSchema(), table)) {
            System.out.printf("Skipping existing table %s%n", targetName);
            return;
        }

        String sourceName = "tpcds." + quotedIdentifier(arguments.sourceSchema()) + "." + quotedTable;
        String sql = "CREATE TABLE " + targetName + " WITH (format = 'PARQUET') AS SELECT " +
                selectList(queryRunner, arguments.sourceSchema(), table) +
                " FROM " + sourceName;

        Instant start = Instant.now();
        System.out.printf("Creating %s from %s ...%n", targetName, sourceName);
        MaterializedResult result = execute(queryRunner, sql);
        long rows = result.getOnlyColumnAsSet().stream()
                .findFirst()
                .map(Long.class::cast)
                .orElse(0L);
        Duration duration = Duration.between(start, Instant.now());
        System.out.printf("Created %s with %,d rows in %s%n", targetName, rows, formatDuration(duration));
    }

    private static List<String> allSourceTables(QueryRunner queryRunner, String sourceSchema)
    {
        MaterializedResult result = execute(queryRunner, "SHOW TABLES FROM tpcds." + quotedIdentifier(sourceSchema));
        List<String> tables = new ArrayList<>();
        result.getMaterializedRows().forEach(row -> tables.add((String) row.getField(0)));
        return tables;
    }

    private static String selectList(QueryRunner queryRunner, String sourceSchema, String table)
    {
        MaterializedResult result = execute(queryRunner, "SHOW COLUMNS FROM tpcds." + quotedIdentifier(sourceSchema) + "." + quotedIdentifier(table));
        List<String> expressions = new ArrayList<>();
        for (var row : result.getMaterializedRows()) {
            String column = (String) row.getField(0);
            String type = ((String) row.getField(1)).toLowerCase(Locale.ENGLISH);
            String quotedColumn = quotedIdentifier(column);

            if (requiresHiveCast(type)) {
                expressions.add("CAST(" + quotedColumn + " AS varchar) AS " + quotedColumn);
                continue;
            }

            expressions.add(quotedColumn);
        }
        return String.join(", ", expressions);
    }

    private static boolean requiresHiveCast(String type)
    {
        return type.equals("time") ||
                type.startsWith("time(") ||
                type.startsWith("time with time zone");
    }

    private static boolean tableExists(QueryRunner queryRunner, String schema, String table)
    {
        MaterializedResult result = execute(queryRunner, "SHOW TABLES FROM hive." + quotedIdentifier(schema) + " LIKE " + stringLiteral(table));
        return result.getRowCount() > 0;
    }

    private static MaterializedResult execute(QueryRunner queryRunner, String sql)
    {
        try {
            return queryRunner.execute(sql);
        }
        catch (RuntimeException exception) {
            throw new RuntimeException("Failed SQL: " + sql, exception);
        }
    }

    private static String quotedIdentifier(String identifier)
    {
        if (identifier.chars().allMatch(character ->
                character == '_' ||
                        character == '$' ||
                        Character.isLetterOrDigit(character))) {
            return identifier;
        }
        return "\"" + identifier.replace("\"", "\"\"") + "\"";
    }

    private static String stringLiteral(String value)
    {
        return "'" + value.replace("'", "''") + "'";
    }

    private static String formatDuration(Duration duration)
    {
        long seconds = duration.toSeconds();
        long minutes = seconds / 60;
        long remainingSeconds = seconds % 60;
        long millis = duration.toMillisPart();
        if (minutes > 0) {
            return "%dm %02d.%03ds".formatted(minutes, remainingSeconds, millis);
        }
        return "%d.%03ds".formatted(remainingSeconds, millis);
    }

    private record Arguments(Path outputRoot, String sourceSchema, String targetSchema, List<String> tables, boolean overwrite, String parquetCompression)
    {
        private static Arguments parse(String[] args)
        {
            Path outputRoot = null;
            String sourceSchema = "sf1";
            String targetSchema = null;
            List<String> tables = List.of();
            boolean overwrite = false;
            String parquetCompression = "GZIP";

            for (int index = 0; index < args.length; index++) {
                String argument = args[index];
                switch (argument) {
                    case "--output-root" -> outputRoot = Path.of(requireValue(args, ++index, argument));
                    case "--source-schema" -> sourceSchema = requireValue(args, ++index, argument);
                    case "--target-schema" -> targetSchema = requireValue(args, ++index, argument);
                    case "--tables" -> tables = parseTables(requireValue(args, ++index, argument));
                    case "--parquet-compression" -> parquetCompression = requireValue(args, ++index, argument).toUpperCase(Locale.ENGLISH);
                    case "--overwrite" -> overwrite = true;
                    case "--help", "-h" -> {
                        printUsage();
                        System.exit(0);
                    }
                    default -> throw new IllegalArgumentException("Unknown argument: " + argument);
                }
            }

            if (outputRoot == null) {
                throw new IllegalArgumentException("--output-root is required");
            }

            if (targetSchema == null) {
                targetSchema = defaultTargetSchema(sourceSchema);
            }

            return new Arguments(outputRoot, sourceSchema, targetSchema, tables, overwrite, parquetCompression);
        }

        private static String requireValue(String[] args, int index, String option)
        {
            if (index >= args.length) {
                throw new IllegalArgumentException("Missing value for " + option);
            }
            return args[index];
        }

        private static List<String> parseTables(String value)
        {
            return Arrays.stream(value.split(","))
                    .map(String::trim)
                    .filter(token -> !token.isEmpty())
                    .map(token -> token.toLowerCase(Locale.ENGLISH))
                    .toList();
        }

        private static String defaultTargetSchema(String sourceSchema)
        {
            if ("sf0.01".equals(sourceSchema)) {
                return "tiny";
            }
            return sourceSchema.replace('.', '_');
        }

        private static void printUsage()
        {
            System.out.println("""
                    Usage: mvn -f tools/tpcds-parquet-dumper/pom.xml exec:java -Dexec.args="..."

                      --output-root <path>     Required root directory for the Hive file metastore and parquet data
                      --source-schema <schema>  TPC-DS source schema, default: sf1
                      --target-schema <schema>  Hive target schema, default: source schema with '.' replaced by '_'
                      --tables <a,b,c>         Comma-separated subset of tables, default: all tables in the source schema
                      --parquet-compression    Parquet compression codec, default: GZIP
                      --overwrite              Drop and recreate target tables if they already exist
                      --help                   Show this message
                    """);
        }
    }
}
