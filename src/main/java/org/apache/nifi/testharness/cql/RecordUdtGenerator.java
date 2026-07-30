package org.apache.nifi.testharness.cql;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Generates CQL {@code CREATE TYPE} statements from a NiFi {@link RecordSchema} - the reverse of
 * what {@link org.apache.nifi.testharness.cql.convert.CqlUdtSchemaMapper}/{@code CqlRecordSchemaMapper}
 * do (CQL -&gt; Avro -&gt; RecordSchema). This is new logic written for this project; nothing in
 * nifi-cql-bundle generates CQL from a schema in this direction.
 *
 * <p>A nested {@code RECORD}-typed field becomes its own nested UDT, referenced via
 * {@code frozen<...>}; statements are returned in dependency order (innermost/nested types first)
 * so pasting them into {@code cqlsh} top-to-bottom just works. A type referenced more than once
 * (the same Avro record schema reused for two fields) is only emitted once.
 */
final class RecordUdtGenerator {

    private RecordUdtGenerator() {
    }

    /**
     * @param schema the top-level record schema to convert
     * @param keyspace if non-null, every generated statement is qualified with this keyspace; if
     *                 null, type names are left unqualified for the caller to run after a {@code USE}
     */
    static List<String> generateCreateTypeStatements(final RecordSchema schema, final String keyspace) {
        final Map<String, String> statementsByTypeName = new LinkedHashMap<>();
        final Set<String> typesCurrentlyBeingResolved = new LinkedHashSet<>();
        resolveRecordType(schema, keyspace, statementsByTypeName, typesCurrentlyBeingResolved);
        return new ArrayList<>(statementsByTypeName.values());
    }

    private static String resolveRecordType(final RecordSchema schema, final String keyspace,
                                             final Map<String, String> statementsByTypeName, final Set<String> typesCurrentlyBeingResolved) {
        final String typeName = schema.getSchemaName()
                .orElseThrow(() -> new IllegalArgumentException("An Avro record has no name, so no UDT name can be derived for it"));
        final String qualifiedName = qualify(typeName, keyspace);

        if (statementsByTypeName.containsKey(typeName)) {
            return qualifiedName;
        }
        if (!typesCurrentlyBeingResolved.add(typeName)) {
            throw new IllegalArgumentException("UDT '" + typeName + "' references itself (directly or through another type) - "
                    + "Cassandra/ScyllaDB UDTs cannot be recursive");
        }

        final List<String> fieldDeclarations = new ArrayList<>();
        for (final RecordField field : schema.getFields()) {
            final String cqlType = resolveCqlType(field.getDataType(), field.getFieldName(), keyspace, statementsByTypeName, typesCurrentlyBeingResolved);
            fieldDeclarations.add(field.getFieldName() + " " + cqlType);
        }

        typesCurrentlyBeingResolved.remove(typeName);
        statementsByTypeName.put(typeName, "CREATE TYPE IF NOT EXISTS " + qualifiedName + " (" + String.join(", ", fieldDeclarations) + ");");
        return qualifiedName;
    }

    private static String resolveCqlType(final DataType dataType, final String fieldName, final String keyspace,
                                          final Map<String, String> statementsByTypeName, final Set<String> typesCurrentlyBeingResolved) {
        final RecordFieldType fieldType = dataType.getFieldType();

        return switch (fieldType) {
            case STRING, ENUM, CHAR -> "text";
            case BOOLEAN -> "boolean";
            case BYTE -> "tinyint";
            case SHORT -> "smallint";
            case INT -> "int";
            case LONG -> "bigint";
            case BIGINT -> "varint";
            case FLOAT -> "float";
            case DOUBLE -> "double";
            case DECIMAL -> "decimal";
            case DATE -> "date";
            case TIME -> "time";
            case TIMESTAMP -> "timestamp";
            case UUID -> "uuid";
            case RECORD -> {
                final RecordDataType recordDataType = (RecordDataType) dataType;
                yield "frozen<" + resolveRecordType(recordDataType.getChildSchema(), keyspace, statementsByTypeName, typesCurrentlyBeingResolved) + ">";
            }
            case ARRAY -> {
                final ArrayDataType arrayDataType = (ArrayDataType) dataType;
                yield "list<" + resolveCqlType(arrayDataType.getElementType(), fieldName, keyspace, statementsByTypeName, typesCurrentlyBeingResolved) + ">";
            }
            // Avro map keys are always strings, so the CQL key type is fixed at text regardless of the
            // Record model's own (unconstrained) map key representation.
            case MAP -> {
                final MapDataType mapDataType = (MapDataType) dataType;
                yield "map<text, " + resolveCqlType(mapDataType.getValueType(), fieldName, keyspace, statementsByTypeName, typesCurrentlyBeingResolved) + ">";
            }
            case CHOICE -> resolveChoiceType((ChoiceDataType) dataType, fieldName, keyspace, statementsByTypeName, typesCurrentlyBeingResolved);
            default -> throw new IllegalArgumentException("Field '" + fieldName + "': Avro/Record type " + fieldType
                    + " has no supported CQL UDT equivalent");
        };
    }

    /**
     * A {@code CHOICE} is how a multi-branch Avro union surfaces (a simple nullable union like
     * {@code ["null", "string"]} is not a CHOICE at all - {@code AvroTypeUtil} resolves it directly to the
     * non-null branch's type and marks the field nullable instead). A CHOICE is only resolvable here if
     * every one of its branches maps to the same CQL type (for example {@code ["string", "null"]} paired
     * with a default, or two branches that happen to coincide); a genuine multi-type union (e.g.
     * {@code ["string", "int"]}) has no single CQL column type to hold it and is rejected with a clear error
     * naming the field.
     */
    private static String resolveChoiceType(final ChoiceDataType choiceDataType, final String fieldName, final String keyspace,
                                             final Map<String, String> statementsByTypeName, final Set<String> typesCurrentlyBeingResolved) {
        final Set<String> distinctCqlTypes = new LinkedHashSet<>();
        for (final DataType subType : choiceDataType.getPossibleSubTypes()) {
            distinctCqlTypes.add(resolveCqlType(subType, fieldName, keyspace, statementsByTypeName, typesCurrentlyBeingResolved));
        }

        if (distinctCqlTypes.size() == 1) {
            return distinctCqlTypes.iterator().next();
        }

        throw new IllegalArgumentException("Field '" + fieldName + "': a multi-type Avro union (" + distinctCqlTypes
                + ") has no single CQL column type to hold it");
    }

    private static String qualify(final String typeName, final String keyspace) {
        return keyspace == null ? typeName : keyspace + "." + typeName;
    }
}
