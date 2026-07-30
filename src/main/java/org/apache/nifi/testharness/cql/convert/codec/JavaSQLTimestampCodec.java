package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.time.Instant;

/**
 * Bridges the driver's default CQL {@code timestamp} codec, which requires {@link Instant}, with
 * {@code java.sql.Timestamp} - the type {@code org.apache.nifi.serialization.record.util.DataTypeUtils}
 * produces for a TIMESTAMP-typed record field. Ported from nifi-cql-bundle's identically-named codec.
 */
public class JavaSQLTimestampCodec implements TypeCodec<Timestamp> {
    private final TypeCodec<Instant> instantCodec;

    public JavaSQLTimestampCodec() {
        this.instantCodec = TypeCodecs.TIMESTAMP;
    }

    @Override
    public GenericType<Timestamp> getJavaType() {
        return GenericType.of(Timestamp.class);
    }

    @Override
    public DataType getCqlType() {
        return instantCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Timestamp value, ProtocolVersion protocolVersion) {
        return value == null ? null : instantCodec.encode(value.toInstant(), protocolVersion);
    }

    @Override
    public Timestamp decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final Instant instant = instantCodec.decode(bytes, protocolVersion);
        return instant == null ? null : Timestamp.from(instant);
    }

    @Override
    public String format(Timestamp value) {
        return value == null ? "NULL" : instantCodec.format(value.toInstant());
    }

    @Override
    public Timestamp parse(String value) {
        final Instant instant = instantCodec.parse(value);
        return instant == null ? null : Timestamp.from(instant);
    }
}
