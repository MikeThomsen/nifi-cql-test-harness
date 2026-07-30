package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.DataTypes;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

/**
 * Binds CQL {@code counter} columns (wire-compatible with {@code bigint}) to any {@link Number},
 * rather than requiring an exact {@link Long}. Ported from nifi-cql-bundle's identically-named codec.
 */
public class FlexibleCounterCodec implements TypeCodec<Number> {

    private final TypeCodec<Long> inner = TypeCodecs.BIGINT;

    @Override
    public GenericType<Number> getJavaType() {
        return GenericType.of(Number.class);
    }

    @Override
    public DataType getCqlType() {
        return DataTypes.COUNTER;
    }

    @Override
    public ByteBuffer encode(Number value, ProtocolVersion protocolVersion) {
        if (value == null) return null;
        return inner.encode(value.longValue(), protocolVersion);
    }

    @Override
    public Number decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return inner.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Number value) {
        return inner.format(value == null ? null : value.longValue());
    }

    @Override
    public Number parse(String value) {
        return inner.parse(value);
    }
}
