package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.sql.Time;
import java.time.LocalDate;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZonedDateTime;

/**
 * Bridges the driver's default CQL {@code time} codec, which requires {@link LocalTime}, with
 * {@code java.sql.Time} - the type {@code org.apache.nifi.serialization.record.util.DataTypeUtils}
 * produces for a TIME-typed record field. Conversion uses the system default time zone to match
 * how {@code DataTypeUtils} itself converts between the two. Ported from nifi-cql-bundle's
 * identically-named codec.
 */
public class JavaSQLTimeCodec implements TypeCodec<Time> {
    private final TypeCodec<LocalTime> localTimeCodec;

    public JavaSQLTimeCodec() {
        this.localTimeCodec = TypeCodecs.TIME;
    }

    @Override
    public GenericType<Time> getJavaType() {
        return GenericType.of(Time.class);
    }

    @Override
    public DataType getCqlType() {
        return localTimeCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Time value, ProtocolVersion protocolVersion) {
        return value == null ? null : localTimeCodec.encode(value.toLocalTime(), protocolVersion);
    }

    @Override
    public Time decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final LocalTime localTime = localTimeCodec.decode(bytes, protocolVersion);
        return localTime == null ? null : toSqlTime(localTime);
    }

    @Override
    public String format(Time value) {
        return value == null ? "NULL" : localTimeCodec.format(value.toLocalTime());
    }

    @Override
    public Time parse(String value) {
        final LocalTime localTime = localTimeCodec.parse(value);
        return localTime == null ? null : toSqlTime(localTime);
    }

    private static Time toSqlTime(final LocalTime localTime) {
        final ZonedDateTime zonedDateTime = localTime.atDate(LocalDate.ofEpochDay(0)).atZone(ZoneId.systemDefault());
        return new Time(zonedDateTime.toInstant().toEpochMilli());
    }
}
