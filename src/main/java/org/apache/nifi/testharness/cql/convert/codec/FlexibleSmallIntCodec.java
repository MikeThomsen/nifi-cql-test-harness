package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code smallint} codec, which only binds an exact
 * {@link Short}, with any value {@code DataTypeUtils} still considers short-compatible. Declared
 * as {@link Object} so it acts as a fallback behind the built-in exact-type codec instead of being
 * rejected as a duplicate registration. Ported from nifi-cql-bundle's identically-named codec.
 */
public class FlexibleSmallIntCodec implements TypeCodec<Object> {
    private final TypeCodec<Short> smallIntCodec;

    public FlexibleSmallIntCodec() {
        this.smallIntCodec = TypeCodecs.SMALLINT;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return smallIntCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : smallIntCodec.encode(DataTypeUtils.toShort(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return smallIntCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : smallIntCodec.format(DataTypeUtils.toShort(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return smallIntCodec.parse(value);
    }
}
