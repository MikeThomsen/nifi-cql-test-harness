package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.sql.Date;
import java.time.Instant;
import java.time.LocalDate;
import java.time.ZoneId;

/**
 * Bridges the driver's default CQL {@code date} codec, which requires {@link LocalDate}, with
 * {@code java.sql.Date} - the type {@code org.apache.nifi.serialization.record.util.DataTypeUtils}
 * produces for a DATE-typed record field. Conversion uses the system default time zone to match
 * how {@code DataTypeUtils} itself converts between the two. Ported from nifi-cql-bundle's
 * identically-named codec.
 */
public class JavaSQLDateCodec implements TypeCodec<Date> {
    private final TypeCodec<LocalDate> localDateCodec;

    public JavaSQLDateCodec() {
        this.localDateCodec = TypeCodecs.DATE;
    }

    @Override
    public GenericType<Date> getJavaType() {
        return GenericType.of(Date.class);
    }

    @Override
    public DataType getCqlType() {
        return localDateCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Date value, ProtocolVersion protocolVersion) {
        return value == null ? null : localDateCodec.encode(toLocalDate(value), protocolVersion);
    }

    @Override
    public Date decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final LocalDate localDate = localDateCodec.decode(bytes, protocolVersion);
        return localDate == null ? null : toSqlDate(localDate);
    }

    @Override
    public String format(Date value) {
        return value == null ? "NULL" : localDateCodec.format(toLocalDate(value));
    }

    @Override
    public Date parse(String value) {
        final LocalDate localDate = localDateCodec.parse(value);
        return localDate == null ? null : toSqlDate(localDate);
    }

    private static LocalDate toLocalDate(final Date value) {
        return LocalDate.ofInstant(Instant.ofEpochMilli(value.getTime()), ZoneId.systemDefault());
    }

    private static Date toSqlDate(final LocalDate localDate) {
        final long epochMillis = localDate.atStartOfDay(ZoneId.systemDefault()).toInstant().toEpochMilli();
        return new Date(epochMillis);
    }
}
