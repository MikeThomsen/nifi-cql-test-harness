package org.apache.nifi.testharness.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.cql.PreparedStatement;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.cql.Row;
import org.apache.nifi.serialization.SimpleRecordSchema;
import org.apache.nifi.serialization.record.MapRecord;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.serialization.record.RecordField;
import org.apache.nifi.serialization.record.RecordFieldType;
import org.apache.nifi.serialization.record.RecordSchema;
import org.apache.nifi.testharness.cql.convert.CqlBindValueConverter;
import org.apache.nifi.testharness.cql.convert.CqlCodecs;
import org.apache.nifi.testharness.cql.convert.CqlRecordSchemaMapper;
import org.apache.nifi.testharness.cql.convert.CqlRowConverter;
import picocli.CommandLine.Command;
import picocli.CommandLine.Option;

import java.net.InetSocketAddress;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.Callable;
import java.util.function.Predicate;

/**
 * Exercises the ported record&lt;-&gt;CQL conversion logic (see
 * {@link org.apache.nifi.testharness.cql.convert}) against a real, already-running container:
 * writes a record covering every supported CQL type through {@link CqlBindValueConverter}, reads
 * it back through {@link CqlRecordSchemaMapper}/{@link CqlRowConverter}, and reports which types
 * round-tripped correctly. Connects to a container started separately via {@code start} - it does
 * not launch one itself.
 *
 * <p>Deliberately does not support TLS-enabled containers: the point of this command is
 * conversion-logic coverage, not exercising SSL, which {@code start --with-ssl} and its own report
 * already cover. Point it at a plain or {@code --with-test-user}-only container.
 */
@Command(
        name = "verify",
        description = {
                "Round-trip every supported CQL type through the ported record<->CQL conversion logic against",
                "an already-running container (start one first with `start`, then pass its reported contact",
                "point here). Creates a dedicated keyspace, writes one record covering every type, reads it",
                "back, and reports which types matched.",
                "",
                "Does not support TLS-enabled containers - point this at a plain or --with-test-user-only",
                "container."
        },
        footer = {
                "",
                "Examples:",
                "  verify --contact-point 127.0.0.1:33193",
                "  verify --contact-point 127.0.0.1:33193 --username admin --password secret"
        }
)
final class VerifyCommand implements Callable<Integer> {

    private static final String LOCAL_DATACENTER = "datacenter1";

    private static final String KEYSPACE = "nifi_cql_verify";

    @Option(names = "--contact-point", required = true, description = "host:port of an already-running container, e.g. the contact point `start` reported.")
    private String contactPoint;

    @Option(names = "--username", description = "Username, if the container requires authentication (start --with-test-user).")
    private String username;

    @Option(names = "--password", description = "Password, if the container requires authentication.")
    private String password;

    /** One CQL column to round-trip: its declaration, the value written, and how to judge the value read back. */
    private record TypeCheck(String label, String columnName, String columnDeclaration, Object writeValue, Predicate<Object> matches) {
    }

    @Override
    public Integer call() throws Exception {
        if ((username == null) != (password == null)) {
            System.err.println("--username and --password must be supplied together");
            return 2;
        }

        final CqlSessionBuilder sessionBuilder = CqlSession.builder()
                .addContactPoint(parseContactPoint(contactPoint))
                .withLocalDatacenter(LOCAL_DATACENTER);
        if (username != null) {
            sessionBuilder.withAuthCredentials(username, password);
        }

        System.out.println("Connecting to " + contactPoint + " ...");
        try (CqlSession session = connectWithRetry(sessionBuilder)) {
            CqlCodecs.registerAll(session);

            System.out.println("Creating keyspace/tables in '" + KEYSPACE + "' ...");
            setUpSchema(session);

            final List<TypeCheck> checks = buildTypeChecks();
            System.out.println("Writing one record covering " + (checks.size() + 1) + " types ...");
            writeRecord(session, checks);

            System.out.println("Reading it back and comparing ...");
            final Map<String, Object> readBack = readRecordValues(session, checks);
            final long counterValue = readCounter(session);

            return report(checks, readBack, counterValue);
        }
    }

    private void setUpSchema(final CqlSession session) {
        session.execute("DROP KEYSPACE IF EXISTS " + KEYSPACE);
        session.execute("CREATE KEYSPACE " + KEYSPACE + " WITH replication = "
                + "{'class': 'NetworkTopologyStrategy', '" + LOCAL_DATACENTER + "': 1}");
        session.execute("CREATE TYPE " + KEYSPACE + ".check_address (street text, city text, zip int)");
        session.execute("CREATE TABLE " + KEYSPACE + ".counter_check (id uuid PRIMARY KEY, hits counter)");
    }

    private List<TypeCheck> buildTypeChecks() {
        final List<TypeCheck> checks = new ArrayList<>();

        // Numeric/boolean scalars: written as Strings to specifically exercise the Flexible*Codec coercion
        // path, not just the driver's own exact-type default codecs.
        checks.add(new TypeCheck("tinyint", "tinyint_col", "tinyint", "5", v -> Byte.valueOf((byte) 5).equals(v)));
        checks.add(new TypeCheck("smallint", "smallint_col", "smallint", "500", v -> Short.valueOf((short) 500).equals(v)));
        checks.add(new TypeCheck("int", "int_col", "int", "42", v -> Integer.valueOf(42).equals(v)));
        checks.add(new TypeCheck("bigint", "bigint_col", "bigint", "9000000000", v -> Long.valueOf(9_000_000_000L).equals(v)));
        checks.add(new TypeCheck("float", "float_col", "float", "3.5", v -> Float.valueOf(3.5f).equals(v)));
        checks.add(new TypeCheck("double", "double_col", "double", "3.14159", v -> Double.valueOf(3.14159).equals(v)));
        checks.add(new TypeCheck("boolean", "boolean_col", "boolean", "true", Boolean.TRUE::equals));

        // text via CharacterCodec: bound as a Character. Read back through the untyped Row#getObject, which -
        // as in nifi-cql-bundle itself - resolves to the driver's own default String-typed text codec rather
        // than CharacterCodec (that codec only ever engages for a typed retrieval, or on write), so the
        // expected read-back is deliberately the String form, not a Character.
        checks.add(new TypeCheck("text (via CharacterCodec on write)", "char_col", "text", 'Q', v -> "Q".equals(v)));

        // date/time/timestamp are written as java.sql.Date/Time/Timestamp via the JavaSQL*Codecs (the type
        // DataTypeUtils itself produces), but - same as the text/CharacterCodec case above - an untyped
        // Row#getObject() read resolves to the driver's own built-in temporal codec (LocalDate/LocalTime/
        // Instant), not the custom write-side codec, so the expected read-back type is deliberately the
        // driver's native temporal type, not the java.sql.* one that was written.
        final Date sqlDate = Date.valueOf(LocalDate.of(2024, 3, 19));
        checks.add(new TypeCheck("date (java.sql.Date via JavaSQLDateCodec on write)", "date_col", "date", sqlDate,
                v -> LocalDate.of(2024, 3, 19).equals(v)));

        final Time sqlTime = Time.valueOf(LocalTime.of(13, 45, 30));
        checks.add(new TypeCheck("time (java.sql.Time via JavaSQLTimeCodec on write)", "time_col", "time", sqlTime,
                v -> LocalTime.of(13, 45, 30).equals(v)));

        final Timestamp sqlTimestamp = Timestamp.from(Instant.now().truncatedTo(java.time.temporal.ChronoUnit.MILLIS));
        checks.add(new TypeCheck("timestamp (java.sql.Timestamp via JavaSQLTimestampCodec on write)", "timestamp_col", "timestamp", sqlTimestamp,
                v -> sqlTimestamp.toInstant().equals(v)));

        final String uuidString = UUID.randomUUID().toString();
        checks.add(new TypeCheck("uuid (String -> UUID)", "uuid_col", "uuid", uuidString,
                v -> UUID.fromString(uuidString).equals(v)));

        checks.add(new TypeCheck("list<text> (Object[] -> List)", "list_col", "list<text>",
                new Object[]{"alpha", "beta"}, v -> List.of("alpha", "beta").equals(v)));
        checks.add(new TypeCheck("set<text> (Object[] -> Set)", "set_col", "set<text>",
                new Object[]{"x", "y"}, v -> Set.of("x", "y").equals(v)));
        checks.add(new TypeCheck("map<text,int>", "map_col", "map<text, int>",
                Map.of("a", 1, "b", 2), v -> Map.of("a", 1, "b", 2).equals(v)));

        final RecordSchema addressSchema = new SimpleRecordSchema(List.of(
                new RecordField("street", RecordFieldType.STRING.getDataType()),
                new RecordField("city", RecordFieldType.STRING.getDataType()),
                new RecordField("zip", RecordFieldType.INT.getDataType())));
        final Map<String, Object> addressValues = new LinkedHashMap<>();
        addressValues.put("street", "1 Infinite Loop");
        addressValues.put("city", "Cupertino");
        addressValues.put("zip", 95014);
        final Record address = new MapRecord(addressSchema, addressValues);
        checks.add(new TypeCheck("UDT (nested Record -> UdtValue)", "udt_col", "frozen<check_address>", address,
                v -> v instanceof com.datastax.oss.driver.api.core.data.UdtValue udt
                        && "1 Infinite Loop".equals(udt.getString("street"))
                        && "Cupertino".equals(udt.getString("city"))
                        && Integer.valueOf(95014).equals(udt.getInt("zip"))));

        return checks;
    }

    private void writeRecord(final CqlSession session, final List<TypeCheck> checks) {
        final StringBuilder createTable = new StringBuilder("CREATE TABLE " + KEYSPACE + ".type_checks (id uuid PRIMARY KEY");
        for (final TypeCheck check : checks) {
            createTable.append(", ").append(check.columnName()).append(' ').append(check.columnDeclaration());
        }
        createTable.append(')');
        session.execute(createTable.toString());

        final List<RecordField> fields = new ArrayList<>();
        fields.add(new RecordField("id", RecordFieldType.STRING.getDataType()));
        final Map<String, Object> values = new LinkedHashMap<>();
        values.put("id", ROW_ID.toString());
        for (final TypeCheck check : checks) {
            fields.add(new RecordField(check.columnName(), RecordFieldType.STRING.getDataType()));
            values.put(check.columnName(), check.writeValue());
        }

        final Record record = new MapRecord(new SimpleRecordSchema(fields), values);

        final List<String> columnNames = new ArrayList<>();
        columnNames.add("id");
        checks.forEach(check -> columnNames.add(check.columnName()));

        final String insertCql = "INSERT INTO " + KEYSPACE + ".type_checks (" + String.join(", ", columnNames) + ") VALUES ("
                + columnNames.stream().map(name -> ":" + name).reduce((a, b) -> a + ", " + b).orElse("") + ")";
        final PreparedStatement insertStatement = session.prepare(insertCql);
        final Object[] bindValues = CqlBindValueConverter.getBindValues(record, insertStatement, columnNames);
        session.execute(insertStatement.bind(bindValues));

        // The counter table can only be modified via an increment, never an INSERT.
        final PreparedStatement counterStatement = session.prepare(
                "UPDATE " + KEYSPACE + ".counter_check SET hits = hits + :hits WHERE id = :id");
        session.execute(counterStatement.bind(7L, ROW_ID));
    }

    private static final UUID ROW_ID = UUID.randomUUID();

    private Map<String, Object> readRecordValues(final CqlSession session, final List<TypeCheck> checks) {
        final ResultSet resultSet = session.execute("SELECT * FROM " + KEYSPACE + ".type_checks WHERE id = " + ROW_ID);
        final RecordSchema schema = CqlRecordSchemaMapper.toRecordSchema(resultSet);
        final Row row = resultSet.one();
        if (row == null) {
            throw new IllegalStateException("No row was written for id " + ROW_ID);
        }
        final Record record = CqlRowConverter.toRecord(row, schema);

        final Map<String, Object> values = new LinkedHashMap<>();
        for (final TypeCheck check : checks) {
            values.put(check.columnName(), record.getValue(check.columnName()));
        }
        return values;
    }

    private long readCounter(final CqlSession session) {
        final ResultSet resultSet = session.execute("SELECT hits FROM " + KEYSPACE + ".counter_check WHERE id = " + ROW_ID);
        final Row row = resultSet.one();
        return row == null ? -1 : row.getLong("hits");
    }

    private int report(final List<TypeCheck> checks, final Map<String, Object> readBack, final long counterValue) {
        int passCount = 0;
        final List<String> lines = new ArrayList<>();

        for (final TypeCheck check : checks) {
            final Object actual = readBack.get(check.columnName());
            final boolean passed = check.matches().test(actual);
            if (passed) {
                passCount++;
            }
            lines.add((passed ? "  [PASS] " : "  [FAIL] ") + check.label() + (passed ? "" : " (got: " + actual + ")"));
        }

        final boolean counterPassed = counterValue == 7L;
        if (counterPassed) {
            passCount++;
        }
        lines.add((counterPassed ? "  [PASS] " : "  [FAIL] ") + "counter (FlexibleCounterCodec, separate table)"
                + (counterPassed ? "" : " (got: " + counterValue + ")"));

        final int total = checks.size() + 1;
        System.out.println();
        System.out.println("Round-trip check: " + passCount + "/" + total + " passed");
        lines.forEach(System.out::println);
        System.out.println();
        System.out.println("Test keyspace '" + KEYSPACE + "' was left in place for inspection.");

        return passCount == total ? 0 : 1;
    }

    private static InetSocketAddress parseContactPoint(final String contactPoint) {
        final int colon = contactPoint.lastIndexOf(':');
        final String host = contactPoint.substring(0, colon);
        final int port = Integer.parseInt(contactPoint.substring(colon + 1));
        return InetSocketAddress.createUnresolved(host, port);
    }

    /**
     * A container's TCP port can accept connections well before its CQL/native-protocol listener
     * is actually ready (observed directly during this project's own {@code start} readiness-check
     * work), so a one-shot {@code build()} can fail against a container that only just reported
     * itself ready. Mirrors {@code TestUserProvisioner}'s retry budget for the same reason.
     */
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
