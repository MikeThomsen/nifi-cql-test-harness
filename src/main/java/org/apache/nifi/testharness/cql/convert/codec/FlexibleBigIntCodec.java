package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import org.apache.nifi.serialization.record.util.DataTypeUtils;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code bigint} codec, which only binds an exact {@link Long},
 * with any value {@code DataTypeUtils} still considers long-compatible (for example a numeric
 * String, or a differently-sized {@link Number}). The Java type is declared as {@link Object}
 * rather than {@code Long} - the built-in codec already claims {@code Long} exactly, so this codec
 * would be silently rejected as a duplicate at registration time if it claimed the same type.
 * Declaring {@code Object} makes it a fallback the registry only reaches once the built-in codec's
 * exact-type match fails. Ported from nifi-cql-bundle's identically-named codec.
 */
public class FlexibleBigIntCodec implements TypeCodec<Object> {
    private final TypeCodec<Long> bigIntCodec;

    public FlexibleBigIntCodec() {
        this.bigIntCodec = TypeCodecs.BIGINT;
    }

    @Override
    public GenericType<Object> getJavaType() {
        return GenericType.of(Object.class);
    }

    @Override
    public DataType getCqlType() {
        return bigIntCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Object value, ProtocolVersion protocolVersion) {
        return value == null ? null : bigIntCodec.encode(DataTypeUtils.toLong(value, getCqlType().toString()), protocolVersion);
    }

    @Override
    public Object decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        return bigIntCodec.decode(bytes, protocolVersion);
    }

    @Override
    public String format(Object value) {
        return value == null ? "NULL" : bigIntCodec.format(DataTypeUtils.toLong(value, getCqlType().toString()));
    }

    @Override
    public Object parse(String value) {
        return bigIntCodec.parse(value);
    }
}
