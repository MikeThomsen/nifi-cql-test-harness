package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code int} codec, which only binds an exact {@link Integer},
 * with any value {@code DataTypeUtils} still considers int-compatible. Declared as {@link Object}
 * so it acts as a fallback behind the built-in exact-type codec instead of being rejected as a
 * duplicate registration. Ported from nifi-cql-bundle's identically-named codec.
 */
public class FlexibleIntCodec implements TypeCodec<Object> {
    private final TypeCodec<Integer> intCodec;

    public FlexibleIntCodec() {
        this.intCodec = TypeCodecs.INT;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return intCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : intCodec.encode(DataTypeUtils.toInteger(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return intCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : intCodec.format(DataTypeUtils.toInteger(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return intCodec.parse(value);
    }
}
