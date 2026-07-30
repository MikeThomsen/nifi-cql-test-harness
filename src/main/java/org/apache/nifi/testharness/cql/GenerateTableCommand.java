package org.apache.nifi.testharness.cql;

import org.apache.avro.Schema;
import org.apache.nifi.avro.AvroTypeUtil;
import org.apache.nifi.serialization.record.RecordSchema;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;

/**
 * Converts an Avro schema file into a CQL {@code CREATE TABLE} statement, using the same
 * Avro -&gt; {@link RecordSchema} -&gt; CQL pipeline as {@code register-type} (via
 * {@link RecordUdtGenerator}), so a nested record field gets exactly the same {@code CREATE TYPE}
 * this project's {@code register-type} command would produce for it, emitted ahead of the table.
 *
 * <p>Field and record names are converted to snake_case for the generated column/table names (see
 * {@link SnakeCaseConverter}); the {@code --primary-key} option itself is expressed in the
 * original Avro field names, not the converted ones, since that's what's actually being read from
 * the schema.
 */
@Command(
        name = "generate-table",
        description = {
                "Converts an Avro schema file into a CQL CREATE TABLE statement and prints it to stdout.",
                "",
                "Column names and the table name are snake_case conversions of the Avro field/record names",
                "(tenantId -> tenant_id, PersonRecord -> person_record). A nested record field becomes its",
                "own CREATE TYPE, printed before the table - the same conversion register-type performs.",
                "",
                "--primary-key uses the ORIGINAL Avro field names (not the converted column names) and",
                "mirrors CQL's own PRIMARY KEY syntax:",
                "  field                  a single-column partition key, no clustering columns",
                "  field1,field2          field1 is the partition key; field2 is a clustering column",
                "  (field1,field2),field3 a composite partition key (field1, field2); field3 is clustering",
                "",
                "A primary key field whose type is an unfrozen collection (Avro array/map) is rejected -",
                "CQL requires those to be frozen to appear in a primary key, which this command does not do.",
                "",
                "Does not connect to a container; this only prints CQL for you to run yourself."
        },
        footer = {
                "",
                "Examples:",
                "  generate-table --schema-file person.avsc --primary-key id",
                "  generate-table --schema-file person.avsc --primary-key \"(tenantId,userId),createdAt\" --keyspace myks"
        }
)
final class GenerateTableCommand implements Callable<Integer> {

    @Option(names = "--schema-file", required = true, description = "Path to an Avro schema (.avsc) file.")
    private Path schemaFile;

    @Option(names = "--primary-key", required = true, description = "Primary key, in Avro field names, using CQL's own PRIMARY KEY syntax (see description).")
    private String primaryKeySpec;

    @Option(names = "--keyspace", description = "If given, qualifies the table name (and any generated UDT names) with this keyspace. If omitted, names are left unqualified.")
    private String keyspace;

    private record ParsedPrimaryKey(List<String> partitionKeyFields, List<String> clusteringKeyFields) {
    }

    @Override
    public Integer call() throws Exception {
        if (!Files.isRegularFile(schemaFile)) {
            System.err.println("--schema-file '" + schemaFile + "' does not exist or is not a file");
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
            System.err.println("'" + schemaFile + "' is a " + avroSchema.getType() + " schema; only a top-level record schema can become a table");
            return 2;
        }

        final RecordSchema recordSchema = AvroTypeUtil.createSchema(avroSchema);

        final ParsedPrimaryKey primaryKey;
        try {
            primaryKey = parsePrimaryKey(primaryKeySpec);
        } catch (final IllegalArgumentException e) {
            System.err.println("--primary-key: " + e.getMessage());
            return 2;
        }

        final RecordUdtGenerator.FieldResolution resolution;
        try {
            resolution = RecordUdtGenerator.resolveFields(recordSchema, keyspace);
        } catch (final IllegalArgumentException e) {
            System.err.println(e.getMessage());
            return 2;
        }

        final List<String> allKeyFields = new ArrayList<>(primaryKey.partitionKeyFields());
        allKeyFields.addAll(primaryKey.clusteringKeyFields());
        try {
            validatePrimaryKeyFields(allKeyFields, resolution.cqlTypesByFieldName());
        } catch (final IllegalArgumentException e) {
            System.err.println("--primary-key: " + e.getMessage());
            return 2;
        }

        resolution.nestedCreateTypeStatements().forEach(System.out::println);
        System.out.println(buildCreateTable(recordSchema, resolution, primaryKey));
        return 0;
    }

    private static ParsedPrimaryKey parsePrimaryKey(final String spec) {
        final String trimmed = spec.trim();
        if (trimmed.isEmpty()) {
            throw new IllegalArgumentException("must not be empty");
        }

        final List<String> partitionKeyFields;
        final String clusteringPart;

        if (trimmed.startsWith("(")) {
            final int closeIndex = trimmed.indexOf(')');
            if (closeIndex < 0) {
                throw new IllegalArgumentException("'" + spec + "' has an unmatched '('");
            }
            partitionKeyFields = splitFieldNames(trimmed.substring(1, closeIndex));
            final String rest = trimmed.substring(closeIndex + 1).trim();
            clusteringPart = rest.startsWith(",") ? rest.substring(1) : rest;
        } else {
            final List<String> allFields = splitFieldNames(trimmed);
            partitionKeyFields = List.of(allFields.get(0));
            clusteringPart = String.join(",", allFields.subList(1, allFields.size()));
        }

        if (partitionKeyFields.isEmpty()) {
            throw new IllegalArgumentException("must specify at least one partition key field");
        }

        final List<String> clusteringKeyFields = clusteringPart.isBlank() ? List.of() : splitFieldNames(clusteringPart);

        final List<String> allFieldsInOrder = new ArrayList<>(partitionKeyFields);
        allFieldsInOrder.addAll(clusteringKeyFields);
        final Set<String> seen = new LinkedHashSet<>();
        for (final String field : allFieldsInOrder) {
            if (!seen.add(field)) {
                throw new IllegalArgumentException("field '" + field + "' appears more than once");
            }
        }

        return new ParsedPrimaryKey(partitionKeyFields, clusteringKeyFields);
    }

    private static List<String> splitFieldNames(final String s) {
        final List<String> fields = Arrays.stream(s.split(","))
                .map(String::trim)
                .filter(field -> !field.isEmpty())
                .toList();
        if (fields.isEmpty()) {
            throw new IllegalArgumentException("no field names found in '" + s + "'");
        }
        return fields;
    }

    private static void validatePrimaryKeyFields(final List<String> keyFields, final Map<String, String> cqlTypesByFieldName) {
        for (final String fieldName : keyFields) {
            final String cqlType = cqlTypesByFieldName.get(fieldName);
            if (cqlType == null) {
                throw new IllegalArgumentException("field '" + fieldName + "' is not present in the Avro schema "
                        + "(available fields: " + cqlTypesByFieldName.keySet() + ")");
            }
            if (cqlType.startsWith("list<") || cqlType.startsWith("map<")) {
                throw new IllegalArgumentException("field '" + fieldName + "' has type " + cqlType
                        + ", which is not allowed in a primary key (CQL requires collections to be frozen there, "
                        + "and this command does not generate frozen collection columns)");
            }
        }
    }

    private String buildCreateTable(final RecordSchema recordSchema, final RecordUdtGenerator.FieldResolution resolution,
                                     final ParsedPrimaryKey primaryKey) {
        final String tableName = SnakeCaseConverter.toSnakeCase(recordSchema.getSchemaName()
                .orElseThrow(() -> new IllegalArgumentException("An Avro record has no name, so no table name can be derived for it")));
        final String qualifiedTableName = keyspace == null ? tableName : keyspace + "." + tableName;

        final List<String> columnDeclarations = new ArrayList<>();
        for (final Map.Entry<String, String> entry : resolution.cqlTypesByFieldName().entrySet()) {
            columnDeclarations.add(SnakeCaseConverter.toSnakeCase(entry.getKey()) + " " + entry.getValue());
        }

        final String partitionKeyColumns = String.join(", ", primaryKey.partitionKeyFields().stream().map(SnakeCaseConverter::toSnakeCase).toList());
        final String primaryKeyClause = primaryKey.clusteringKeyFields().isEmpty()
                ? "PRIMARY KEY ((" + partitionKeyColumns + "))"
                : "PRIMARY KEY ((" + partitionKeyColumns + "), "
                        + String.join(", ", primaryKey.clusteringKeyFields().stream().map(SnakeCaseConverter::toSnakeCase).toList()) + ")";

        return "CREATE TABLE IF NOT EXISTS " + qualifiedTableName + " (" + String.join(", ", columnDeclarations)
                + ", " + primaryKeyClause + ");";
    }
}
