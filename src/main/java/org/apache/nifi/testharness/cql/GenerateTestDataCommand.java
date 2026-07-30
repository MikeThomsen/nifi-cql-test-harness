package org.apache.nifi.testharness.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.io.BufferedWriter;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedHashSet;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Generates fake test data (via {@link TestDataGenerator}) matching an Avro schema and either
 * writes it directly to a running container or dumps it as literal CQL {@code INSERT} statements
 * to a file. Every statement is generated as literal CQL text (see {@link CqlLiteralFormatter})
 * rather than bound through a {@code PreparedStatement} - the same text is used whether it's being
 * written to a file or executed, so dry-run/archive output is exactly what actually got sent to the
 * database in a live run, not a separately-maintained approximation of it.
 */
@Command(
        name = "generate-test-data",
        description = {
                "Generates fake test data matching an Avro schema and writes it to a running container by",
                "default, using DataFaker for realistic-looking values (a field named e.g. \"email\" or \"city\"",
                "gets a matching generator; anything else falls back to generic text).",
                "",
                "--dry-run generates the CQL and writes it to a file instead of touching a container:",
                "  <RecordName>-dry-run<yyyy-MM-dd-HH-mm-ss>.cql, in the current directory.",
                "--archive-generated-cql-to-path <dir> keeps the normal live write, but additionally writes the",
                "same generated CQL to a file in <dir>, using the same naming convention (with \"archive\" in",
                "place of \"dry-run\"). Mutually exclusive with --dry-run, which already implies file output.",
                "",
                "--contact-point and --keyspace are required unless --dry-run is set.",
                "",
                "DATE/TIME/TIMESTAMP fields default to a per-field sequence (DATE +1 day, TIME +1 second,",
                "TIMESTAMP +1 second per record, starting from today/00:00:00/now respectively) rather than",
                "random values, so a column like \"createdAt\" reads as a plausible time series across the",
                "generated rows. Pass --randomize-temporal-fields to get random values for those instead.",
                "",
                "If the schema contains a nested record, this prints a non-blocking warning reminding you to",
                "run register-type first (INSERT statements referencing an unregistered UDT will fail) - it",
                "does not check a live container for whether that's actually been done."
        },
        footer = {
                "",
                "Examples:",
                "  generate-test-data --schema-file person.avsc --contact-point 127.0.0.1:33193 --keyspace myks",
                "  generate-test-data --schema-file person.avsc --dry-run --record-count 100",
                "  generate-test-data --schema-file person.avsc --contact-point 127.0.0.1:33193 --keyspace myks "
                        + "--archive-generated-cql-to-path ./archives"
        }
)
final class GenerateTestDataCommand implements Callable<Integer> {

    private static final String LOCAL_DATACENTER = "datacenter1";

    private static final DateTimeFormatter FILE_TIMESTAMP_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd-HH-mm-ss");

    private static final int PROGRESS_INTERVAL = 500;

    @Option(names = "--schema-file", required = true, description = "Path to an Avro schema (.avsc) file.")
    private Path schemaFile;

    @Option(names = "--record-count", defaultValue = "5000", description = "Number of records to generate. Default: ${DEFAULT-VALUE}.")
    private int recordCount;

    @Option(names = "--dry-run", description = "Write generated CQL to a file instead of a container. Mutually exclusive with --archive-generated-cql-to-path.")
    private boolean dryRun;

    @Option(names = "--archive-generated-cql-to-path", description = "In addition to writing live, also save the generated CQL to a file in this directory.")
    private Path archivePath;

    @Option(names = "--contact-point", description = "host:port of an already-running container. Required unless --dry-run.")
    private String contactPoint;

    @Option(names = "--username", description = "Username, if the container requires authentication.")
    private String username;

    @Option(names = "--password", description = "Password, if the container requires authentication.")
    private String password;

    @Option(names = "--keyspace", description = "Target keyspace. Required unless --dry-run.")
    private String keyspace;

    @Option(names = "--randomize-temporal-fields",
            description = "Generate random DATE/TIME/TIMESTAMP values instead of the default per-field sequence.")
    private boolean randomizeTemporalFields;

    @Override
    public Integer call() throws Exception {
        if (!Files.isRegularFile(schemaFile)) {
            System.err.println("--schema-file '" + schemaFile + "' does not exist or is not a file");
            return 2;
        }
        if ((username == null) != (password == null)) {
            System.err.println("--username and --password must be supplied together");
            return 2;
        }
        if (dryRun && archivePath != null) {
            System.err.println("--dry-run and --archive-generated-cql-to-path are mutually exclusive - --dry-run already writes to a file");
            return 2;
        }
        if (!dryRun && contactPoint == null) {
            System.err.println("--contact-point is required unless --dry-run is set");
            return 2;
        }
        if (!dryRun && keyspace == null) {
            System.err.println("--keyspace is required unless --dry-run is set");
            return 2;
        }
        if (recordCount <= 0) {
            System.err.println("--record-count must be positive");
            return 2;
        }

        final Schema avroSchema;
        try {
            avroSchema = new Schema.Parser().parse(Files.readString(schemaFile));
        } catch (final Exception e) {
            System.err.println("Could not parse '" + schemaFile + "' as an Avro schema: " + e.getMessage());
            return 2;
        }
        if (avroSchema.getType() != Schema.Type.RECORD) {
            System.err.println("'" + schemaFile + "' is a " + avroSchema.getType() + " schema; only a top-level record schema can generate test data");
            return 2;
        }

        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);
        final String recordName = recordSchema.getSchemaName()
                .orElseThrow(() -> new IllegalArgumentException("An Avro record has no name, so no table name can be derived for it"));

        final Set<String> nestedTypeNames = findNestedRecordTypeNames(recordSchema);
        if (!nestedTypeNames.isEmpty()) {
            System.err.println("Warning: this schema references nested record type(s) " + nestedTypeNames
                    + " - make sure you've run register-type for them first, or the generated INSERT statements will fail.");
        }

        final String tableName = SnakeCaseConverter.toSnakeCase(recordName);
        final String qualifiedTableName = keyspace == null ? tableName : keyspace + "." + tableName;

        return dryRun ? runDryRun(recordSchema, qualifiedTableName, recordName) : runLiveWrite(recordSchema, qualifiedTableName, recordName);
    }

    private Integer runDryRun(final RecordSchema recordSchema, final String qualifiedTableName, final String recordName) throws IOException {
        final Path outputFile = Path.of(buildOutputFileName(recordName, "dry-run"));
        final TestDataGenerator generator = new TestDataGenerator(randomizeTemporalFields);

        try (BufferedWriter writer = Files.newBufferedWriter(outputFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING)) {
            for (int i = 0; i < recordCount; i++) {
                final Record record = generator.generate(recordSchema);
                writer.write(CqlLiteralFormatter.formatInsert(qualifiedTableName, recordSchema, record));
                writer.newLine();
            }
        }

        System.out.println("Wrote " + recordCount + " generated INSERT statement(s) to " + outputFile.toAbsolutePath());
        return 0;
    }

    private Integer runLiveWrite(final RecordSchema recordSchema, final String qualifiedTableName, final String recordName) throws Exception {
        final CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .addContactPoint(parseContactPoint(contactPoint))
                .withLocalDatacenter(LOCAL_DATACENTER);
        if (username != null) {
            sessionBuilder.withAuthCredentials(username, password);
        }

        final TestDataGenerator generator = new TestDataGenerator(randomizeTemporalFields);
        Path archiveFile = null;
        BufferedWriter archiveWriter = null;

        System.out.println("Connecting to " + contactPoint + " ...");
        try (CqlSession session = connectWithRetry(sessionBuilder)) {
            if (archivePath != null) {
                Files.createDirectories(archivePath);
                archiveFile = archivePath.resolve(buildOutputFileName(recordName, "archive"));
                archiveWriter = Files.newBufferedWriter(archiveFile, StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING);
            }

            System.out.println("Writing " + recordCount + " generated record(s) to " + qualifiedTableName + " ...");
            for (int i = 0; i < recordCount; i++) {
                final Record record = generator.generate(recordSchema);
                final String statement = CqlLiteralFormatter.formatInsert(qualifiedTableName, recordSchema, record);
                session.execute(statement);
                if (archiveWriter != null) {
                    archiveWriter.write(statement);
                    archiveWriter.newLine();
                }
                if ((i + 1) % PROGRESS_INTERVAL == 0) {
                    System.out.println("  " + (i + 1) + "/" + recordCount);
                }
            }
        } finally {
            if (archiveWriter != null) {
                archiveWriter.close();
            }
        }

        System.out.println("Wrote " + recordCount + " record(s) to " + qualifiedTableName);
        if (archiveFile != null) {
            System.out.println("Archived generated CQL to " + archiveFile.toAbsolutePath());
        }
        return 0;
    }

    private static String buildOutputFileName(final String recordName, final String modeLabel) {
        return recordName + "-" + modeLabel + LocalDateTime.now().format(FILE_TIMESTAMP_FORMATTER) + ".cql";
    }

    private static Set<String> findNestedRecordTypeNames(final RecordSchema schema) {
        final Set<String> found = new LinkedHashSet<>();
        final Set<String> visiting = new LinkedHashSet<>();
        for (final RecordField field : schema.getFields()) {
            collectNestedRecordTypeNames(field.getDataType(), found, visiting);
        }
        return found;
    }

    private static void collectNestedRecordTypeNames(final DataType dataType, final Set<String> found, final Set<String> visiting) {
        switch (dataType.getFieldType()) {
            case RECORD -> {
                final RecordSchema childSchema = ((RecordDataType) dataType).getChildSchema();
                final String typeName = childSchema.getSchemaName().orElse(null);
                if (typeName != null && !visiting.add(typeName)) {
                    return;
                }
                if (typeName != null) {
                    found.add(typeName);
                }
                for (final RecordField field : childSchema.getFields()) {
                    collectNestedRecordTypeNames(field.getDataType(), found, visiting);
                }
                if (typeName != null) {
                    visiting.remove(typeName);
                }
            }
            case ARRAY -> collectNestedRecordTypeNames(((ArrayDataType) dataType).getElementType(), found, visiting);
            case MAP -> collectNestedRecordTypeNames(((MapDataType) dataType).getValueType(), found, visiting);
            case CHOICE -> ((ChoiceDataType) dataType).getPossibleSubTypes().forEach(subType -> collectNestedRecordTypeNames(subType, found, visiting));
            default -> {
            }
        }
    }

    private static InetSocketAddress parseContactPoint(final String contactPoint) {
        final int colon = contactPoint.lastIndexOf(':');
        final String host = contactPoint.substring(0, colon);
        final int port = Integer.parseInt(contactPoint.substring(colon + 1));
        return InetSocketAddress.createUnresolved(host, port);
    }

    /** Same rationale and retry budget as the identical helper in VerifyCommand: a container's TCP port can
     * accept connections before its CQL listener is actually ready. */
    private static CqlSession connectWithRetry(final CqlSessionBuilder sessionBuilder) throws InterruptedException {
        final int connectAttempts = 40;
        final long retryDelayMillis = 3_000L;

        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= connectAttempts; attempt++) {
            try {
                return sessionBuilder.build();
            } catch (final RuntimeException e) {
                lastFailure = e;
                if (attempt < connectAttempts) {
                    Thread.sleep(retryDelayMillis);
                }
            }
        }
        throw new IllegalStateException("Could not connect after " + connectAttempts + " attempts", lastFailure);
    }
}
