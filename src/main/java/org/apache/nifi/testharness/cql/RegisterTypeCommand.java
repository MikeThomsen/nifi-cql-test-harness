package org.apache.nifi.testharness.cql;

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.RecordSchema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.Callable;

/**
 * Converts an Avro schema file into CQL {@code CREATE TYPE} statement(s), using the NiFi Record
 * API as the intermediate representation - the same {@code AvroTypeUtil.createSchema} call
 * {@code CqlRecordSchemaMapper} uses in the opposite direction (CQL -&gt; Avro -&gt; RecordSchema) -
 * rather than walking the Avro {@link Schema} directly. A nested Avro record becomes its own
 * nested UDT, printed before the type that references it.
 *
 * <p>Prints the generated CQL to stdout only; it does not connect to or execute against a
 * container.
 */
@Command(
        name = "register-type",
        description = {
                "Converts an Avro schema file into CQL CREATE TYPE statement(s) and prints them to stdout.",
                "",
                "The Avro schema is read via NiFi's own Record API (AvroTypeUtil.createSchema), the same",
                "conversion CqlRecordSchemaMapper uses in the opposite direction, then walked to emit one",
                "CREATE TYPE per nested record - nested types are printed first, so pasting the output into",
                "cqlsh top-to-bottom works without reordering.",
                "",
                "Does not connect to a container; this only prints CQL for you to run yourself."
        },
        footer = {
                "",
                "Examples:",
                "  register-type --schema-file address.avsc",
                "  register-type --schema-file address.avsc --keyspace myks"
        }
)
final class RegisterTypeCommand implements Callable<Integer> {

    @Option(names = "--schema-file", required = true, description = "Path to an Avro schema (.avsc) file.")
    private Path schemaFile;

    @Option(names = "--keyspace", description = "If given, qualifies every generated type name with this keyspace. If omitted, type names are left unqualified.")
    private String keyspace;

    @Override
    public Integer call() throws Exception {
        if (!Files.isRegularFile(schemaFile)) {
            System.err.println("--schema-file '" + schemaFile + "' does not exist or is not a file");
            return 2;
        }

        final String schemaText = Files.readString(schemaFile);
        final Schema avroSchema;
        try {
            avroSchema = new Schema.Parser().parse(schemaText);
        } catch (final Exception e) {
            System.err.println("Could not parse '" + schemaFile + "' as an Avro schema: " + e.getMessage());
            return 2;
        }
        if (avroSchema.getType() != Schema.Type.RECORD) {
            System.err.println("'" + schemaFile + "' is a " + avroSchema.getType() + " schema; only a top-level record schema can become a UDT");
            return 2;
        }

        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        final List<String> statements;
        try {
            statements = RecordUdtGenerator.generateCreateTypeStatements(recordSchema, keyspace);
        } catch (final IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return 2;
        }

        statements.forEach(System.out::println);
        return 0;
    }
}
