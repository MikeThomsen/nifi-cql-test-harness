package org.apache.nifi.testharness.cql;

import net.datafaker.Faker;
import org.apache.nifi.serialization.record.DataType;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.serialization.record.type.ArrayDataType;
import org.apache.nifi.serialization.record.type.ChoiceDataType;
import org.apache.nifi.serialization.record.type.DecimalDataType;
import org.apache.nifi.serialization.record.type.EnumDataType;
import org.apache.nifi.serialization.record.type.MapDataType;
import org.apache.nifi.serialization.record.type.RecordDataType;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

/**
 * Generates fake {@link Record} instances matching a {@link RecordSchema}, using a generator
 * appropriate to each {@link RecordFieldType}. A {@code STRING}/{@code CHAR} field whose name
 * suggests a recognizable kind of data (email, name, city, ...) gets a matching DataFaker
 * generator; anything else falls back to generic lorem text. A nested {@code RECORD} field
 * recurses into a real nested {@link Record}, matching the shape {@link CqlLiteralFormatter}
 * (and, elsewhere in this project, {@code CqlBindValueConverter}) already expect for UDT-typed
 * values.
 *
 * <p>{@code DATE}/{@code TIME}/{@code TIMESTAMP} fields default to a deterministic, per-field
 * sequence (DATE +1 day, TIME +1 second, TIMESTAMP +1 second per generated record) rather than
 * random values, so that e.g. a "createdAt" column across 5000 rows reads as a plausible time
 * series instead of scattered noise. Pass {@code randomizeTemporalFields=true} to fall back to
 * random values for those three types instead.
 */
final class TestDataGenerator {

    private static final int MAX_COLLECTION_SIZE = 4;
    private static final int MAX_PAST_DAYS = 3650;

    private final Faker faker = new Faker();
    private final boolean randomizeTemporalFields;

    private final Map<String, LocalDate> dateSequenceByField = new HashMap<>();
    private final Map<String, LocalTime> timeSequenceByField = new HashMap<>();
    private final Map<String, Instant> timestampSequenceByField = new HashMap<>();

    TestDataGenerator(final boolean randomizeTemporalFields) {
        this.randomizeTemporalFields = randomizeTemporalFields;
    }

    Record generate(final RecordSchema schema) {
        final Map<String, Object> values = new LinkedHashMap<>();
        for (final RecordField field : schema.getFields()) {
            values.put(field.getFieldName(), generateValue(field.getDataType(), field.getFieldName()));
        }
        return new MapRecord(schema, values);
    }

    private Object generateValue(final DataType dataType, final String fieldName) {
        return switch (dataType.getFieldType()) {
            case STRING, CHAR -> generateString(fieldName);
            case BOOLEAN -> generateBoolean();
            case BYTE -> generateByte();
            case SHORT -> generateShort();
            case INT -> generateInt();
            case LONG, BIGINT -> generateLong();
            case FLOAT -> generateFloat();
            case DOUBLE -> generateDouble();
            case DECIMAL -> generateDecimal((DecimalDataType) dataType);
            case DATE -> generateDate(fieldName);
            case TIME -> generateTime(fieldName);
            case TIMESTAMP -> generateTimestamp(fieldName);
            case UUID -> generateUuid();
            case ENUM -> generateEnum((EnumDataType) dataType);
            case RECORD -> generate(((RecordDataType) dataType).getChildSchema());
            case ARRAY -> generateArray((ArrayDataType) dataType, fieldName);
            case MAP -> generateMap((MapDataType) dataType, fieldName);
            // Only a CHOICE all of whose branches share one CQL type ever survives RecordUdtGenerator's own
            // validation elsewhere in this project, so consistently using the first branch here - rather than
            // a different random one per call - keeps generation and CqlLiteralFormatter in agreement about
            // which branch's shape a given value has.
            case CHOICE -> generateValue(((ChoiceDataType) dataType).getPossibleSubTypes().get(0), fieldName);
            default -> throw new IllegalArgumentException("Cannot generate test data for Avro/Record type "
                    + dataType.getFieldType() + " (field '" + fieldName + "')");
        };
    }

    private String generateString(final String fieldName) {
        final String name = fieldName.toLowerCase(Locale.ROOT);
        if (name.contains("email")) {
            return faker.internet().emailAddress();
        }
        if (name.contains("phone")) {
            return faker.phoneNumber().phoneNumber();
        }
        if (name.contains("url") || name.contains("website")) {
            return faker.internet().url();
        }
        if (name.contains("username")) {
            return faker.name().username();
        }
        if (name.contains("company")) {
            return faker.company().name();
        }
        if (name.contains("street") || name.contains("address")) {
            return faker.address().streetAddress();
        }
        if (name.contains("city")) {
            return faker.address().city();
        }
        if (name.contains("state")) {
            return faker.address().state();
        }
        if (name.contains("zip") || name.contains("postal")) {
            return faker.address().zipCode();
        }
        if (name.contains("country")) {
            return faker.address().country();
        }
        if (name.contains("name")) {
            return faker.name().fullName();
        }
        return faker.lorem().word();
    }

    private boolean generateBoolean() {
        return faker.bool().bool();
    }

    private byte generateByte() {
        return (byte) faker.number().numberBetween(-128, 127);
    }

    private short generateShort() {
        return (short) faker.number().numberBetween(-32_768, 32_767);
    }

    private int generateInt() {
        return faker.number().numberBetween(0, 1_000_000);
    }

    private long generateLong() {
        return faker.number().numberBetween(0L, 1_000_000_000L);
    }

    private float generateFloat() {
        return (float) faker.number().randomDouble(2, 0, 10_000);
    }

    private double generateDouble() {
        return faker.number().randomDouble(2, 0, 1_000_000);
    }

    private UUID generateUuid() {
        return UUID.randomUUID();
    }

    private BigDecimal generateDecimal(final DecimalDataType decimalDataType) {
        final int scale = decimalDataType.getScale();
        final int integerDigits = Math.max(1, decimalDataType.getPrecision() - scale);
        final long maxIntegerValue = (long) Math.pow(10, integerDigits) - 1;
        final double value = faker.number().randomDouble(scale, 0, maxIntegerValue);
        return BigDecimal.valueOf(value).setScale(scale, RoundingMode.HALF_UP);
    }

    private Date generateDate(final String fieldName) {
        if (randomizeTemporalFields) {
            return new Date(faker.date().past(MAX_PAST_DAYS, TimeUnit.DAYS).getTime());
        }
        final LocalDate next = dateSequenceByField.merge(fieldName, LocalDate.now(), (current, initial) -> current.plusDays(1));
        return Date.valueOf(next);
    }

    private Time generateTime(final String fieldName) {
        if (randomizeTemporalFields) {
            return Time.valueOf(LocalTime.of(
                    faker.number().numberBetween(0, 24),
                    faker.number().numberBetween(0, 60),
                    faker.number().numberBetween(0, 60)));
        }
        final LocalTime next = timeSequenceByField.merge(fieldName, LocalTime.MIDNIGHT, (current, initial) -> current.plusSeconds(1));
        return Time.valueOf(next);
    }

    private Timestamp generateTimestamp(final String fieldName) {
        if (randomizeTemporalFields) {
            return new Timestamp(faker.date().past(MAX_PAST_DAYS, TimeUnit.DAYS).getTime());
        }
        final Instant next = timestampSequenceByField.merge(fieldName, Instant.now(), (current, initial) -> current.plusSeconds(1));
        return Timestamp.from(next);
    }

    private String generateEnum(final EnumDataType enumDataType) {
        final List<String> symbols = enumDataType.getEnums();
        return symbols.get(faker.number().numberBetween(0, symbols.size()));
    }

    private Object generateArray(final ArrayDataType arrayDataType, final String fieldName) {
        final int size = faker.number().numberBetween(1, MAX_COLLECTION_SIZE + 1);
        final Object[] elements = new Object[size];
        for (int i = 0; i < size; i++) {
            elements[i] = generateValue(arrayDataType.getElementType(), fieldName);
        }
        return elements;
    }

    private Map<String, Object> generateMap(final MapDataType mapDataType, final String fieldName) {
        final int size = faker.number().numberBetween(1, MAX_COLLECTION_SIZE + 1);
        final Map<String, Object> map = new LinkedHashMap<>();
        for (int i = 0; i < size; i++) {
            // Avro map keys are always strings; a short lorem word keeps generated keys readable.
            map.put(faker.lorem().word() + "-" + i, generateValue(mapDataType.getValueType(), fieldName));
        }
        return map;
    }
}
