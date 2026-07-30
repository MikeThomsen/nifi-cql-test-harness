package org.apache.nifi.testharness.cql.convert.codec;

import com.datastax.oss.driver.api.core.ProtocolVersion;
import com.datastax.oss.driver.api.core.type.DataType;
import com.datastax.oss.driver.api.core.type.codec.TypeCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;

import java.nio.ByteBuffer;

/**
 * Bridges the driver's default CQL {@code text} codec, which requires {@link String}, with
 * {@link Character} - the type {@code org.apache.nifi.serialization.record.util.DataTypeUtils}
 * produces for a CHAR-typed record field. Cassandra has no dedicated single-character type, so
 * {@code text} is the natural fit; this codec represents a {@code Character} as a one-character
 * CQL string. Ported from nifi-cql-bundle's identically-named codec.
 */
public class CharacterCodec implements TypeCodec<Character> {
    private final TypeCodec<String> stringCodec;

    public CharacterCodec() {
        this.stringCodec = TypeCodecs.TEXT;
    }

    @Override
    public GenericType<Character> getJavaType() {
        return GenericType.of(Character.class);
    }

    @Override
    public DataType getCqlType() {
        return stringCodec.getCqlType();
    }

    @Override
    public ByteBuffer encode(Character value, ProtocolVersion protocolVersion) {
        return value == null ? null : stringCodec.encode(value.toString(), protocolVersion);
    }

    @Override
    public Character decode(ByteBuffer bytes, ProtocolVersion protocolVersion) {
        final String string = stringCodec.decode(bytes, protocolVersion);
        return (string == null || string.isEmpty()) ? null : string.charAt(0);
    }

    @Override
    public String format(Character value) {
        return value == null ? "NULL" : stringCodec.format(value.toString());
    }

    @Override
    public Character parse(String value) {
        final String string = stringCodec.parse(value);
        return (string == null || string.isEmpty()) ? null : string.charAt(0);
    }
}
