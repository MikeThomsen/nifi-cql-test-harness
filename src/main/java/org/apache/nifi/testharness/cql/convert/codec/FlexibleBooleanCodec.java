package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code boolean} codec, which only binds an exact
 * {@link Boolean}, with any value {@code DataTypeUtils} still considers boolean-compatible
 * (for example a {@code "true"}/{@code "false"} String). Declared as {@link Object} rather than
 * {@code Boolean} so it acts as a fallback behind the built-in exact-type codec instead of being
 * rejected as a duplicate registration. Ported from nifi-cql-bundle's identically-named codec.
 */
public class FlexibleBooleanCodec implements TypeCodec<Object> {
    private final TypeCodec<Boolean> booleanCodec;

    public FlexibleBooleanCodec() {
        this.booleanCodec = TypeCodecs.BOOLEAN;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return booleanCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : booleanCodec.encode(DataTypeUtils.toBoolean(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return booleanCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : booleanCodec.format(DataTypeUtils.toBoolean(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return booleanCodec.parse(value);
    }
}
