package org.apache.nifi.testharness.cql;

import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;

/**
 * Renders a generated {@link Record} as a literal CQL {@code INSERT} statement - the text form
 * {@code generate-test-data} needs for both the dry-run/archive file output and (since that command
 * executes each statement as literal text rather than binding through a PreparedStatement) the live
 * write path. Column names use the same snake_case conversion as {@code generate-table}; a nested
 * UDT literal's own field names stay in their original Avro casing, matching what
 * {@link RecordUdtGenerator} actually declares in the corresponding {@code CREATE TYPE}.
 */
final class CqlLiteralFormatter {

    private static final DateTimeFormatter DATE_FORMATTER = DateTimeFormatter.ISO_LOCAL_DATE;

    private static final DateTimeFormatter TIME_FORMATTER = DateTimeFormatter.ofPattern("HH:mm:ss");

    private CqlLiteralFormatter() {
    }

    static String formatInsert(final String qualifiedTableName, final RecordSchema schema, final Record record) {
        final List<String> columnNames = new ArrayList<>();
        final List<String> valueLiterals = new ArrayList<>();

        for (final RecordField field : schema.getFields()) {
            columnNames.add(SnakeCaseConverter.toSnakeCase(field.getFieldName()));
            valueLiterals.add(formatValue(field.getDataType(), record.getValue(field.getFieldName())));
        }

        return "INSERT INTO " + qualifiedTableName + " (" + String.join(", ", columnNames) + ") VALUES ("
                + String.join(", ", valueLiterals) + ");";
    }

    private static String formatValue(final DataType dataType, final Object value) {
        if (value == null) {
            return "null";
        }

        return switch (dataType.getFieldType()) {
            case STRING, CHAR, ENUM -> quoteText(value.toString());
            case BOOLEAN, BYTE, SHORT, INT, LONG, BIGINT, FLOAT, DOUBLE, DECIMAL, UUID -> value.toString();
            case DATE -> quoteText(((Date) value).toLocalDate().format(DATE_FORMATTER));
            case TIME -> quoteText(((Time) value).toLocalTime().format(TIME_FORMATTER));
            case TIMESTAMP -> quoteText(((Timestamp) value).toInstant().toString());
            case RECORD -> formatUdtLiteral((RecordDataType) dataType, (Record) value);
            case ARRAY -> formatListLiteral((ArrayDataType) dataType, value);
            case MAP -> formatMapLiteral((MapDataType) dataType, (Map<?, ?>) value);
            // See TestDataGenerator's identical CHOICE handling for why the first branch is used consistently.
            case CHOICE -> formatValue(((ChoiceDataType) dataType).getPossibleSubTypes().get(0), value);
            default -> throw new IllegalArgumentException("Cannot format a CQL literal for type " + dataType.getFieldType());
        };
    }

    private static String formatUdtLiteral(final RecordDataType dataType, final Record value) {
        final RecordSchema childSchema = dataType.getChildSchema();
        final List<String> fieldLiterals = new ArrayList<>();
        for (final RecordField field : childSchema.getFields()) {
            fieldLiterals.add(field.getFieldName() + ": " + formatValue(field.getDataType(), value.getValue(field.getFieldName())));
        }
        return "{" + String.join(", ", fieldLiterals) + "}";
    }

    private static String formatListLiteral(final ArrayDataType dataType, final Object value) {
        final DataType elementType = dataType.getElementType();
        final List<?> elements = value instanceof Object[] array ? Arrays.asList(array) : (List<?>) value;
        final List<String> elementLiterals = elements.stream().map(element -> formatValue(elementType, element)).toList();
        return "[" + String.join(", ", elementLiterals) + "]";
    }

    private static String formatMapLiteral(final MapDataType dataType, final Map<?, ?> value) {
        final DataType valueType = dataType.getValueType();
        final List<String> entryLiterals = new ArrayList<>();
        for (final Map.Entry<?, ?> entry : value.entrySet()) {
            entryLiterals.add(quoteText(entry.getKey().toString()) + ": " + formatValue(valueType, entry.getValue()));
        }
        return "{" + String.join(", ", entryLiterals) + "}";
    }

    private static String quoteText(final String text) {
        return "'" + text.replace("'", "''") + "'";
    }
}
