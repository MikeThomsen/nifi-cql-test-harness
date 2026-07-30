package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code double} codec, which only binds an exact {@link Double},
 * with any value {@code DataTypeUtils} still considers double-compatible. Declared as
 * {@link Object} so it acts as a fallback behind the built-in exact-type codec instead of being
 * rejected as a duplicate registration. Ported from nifi-cql-bundle's identically-named codec.
 */
public class FlexibleDoubleCodec implements TypeCodec<Object> {
    private final TypeCodec<Double> doubleCodec;

    public FlexibleDoubleCodec() {
        this.doubleCodec = TypeCodecs.DOUBLE;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return doubleCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : doubleCodec.encode(DataTypeUtils.toDouble(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return doubleCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : doubleCodec.format(DataTypeUtils.toDouble(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return doubleCodec.parse(value);
    }
}
