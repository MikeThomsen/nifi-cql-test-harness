package org.apache.nifi.testharness.cql.convert;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import org.apache.nifi.testharness.cql.convert.codec.CharacterCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleBigIntCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleBooleanCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleCounterCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleDoubleCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleFloatCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleIntCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleSmallIntCodec;
import org.apache.nifi.testharness.cql.convert.codec.FlexibleTinyIntCodec;
import org.apache.nifi.testharness.cql.convert.codec.JavaSQLDateCodec;
import org.apache.nifi.testharness.cql.convert.codec.JavaSQLTimeCodec;
import org.apache.nifi.testharness.cql.convert.codec.JavaSQLTimestampCodec;

/**
 * Registers the record-compatible codecs on a {@link CqlSession}'s codec registry, the same set
 * nifi-cql-bundle's {@code CassandraCQLExecutionService} registers when it opens a session -
 * without these, values NiFi's own {@code Record}/{@code DataTypeUtils} produce (e.g. a
 * {@code java.sql.Date} for a DATE column, or a numeric String for an {@code int} column) fail to
 * bind against the driver's default exact-type-only codecs.
 */
public final class CqlCodecs {

    private CqlCodecs() {
    }

    public static void registerAll(final CqlSession session) {
        final MutableCodecRegistry codecRegistry = (MutableCodecRegistry) session.getContext().getCodecRegistry();

        codecRegistry.register(new JavaSQLTimestampCodec());
        codecRegistry.register(new JavaSQLDateCodec());
        codecRegistry.register(new JavaSQLTimeCodec());
        codecRegistry.register(new CharacterCodec());
        codecRegistry.register(new FlexibleCounterCodec());
        codecRegistry.register(new FlexibleBooleanCodec());
        codecRegistry.register(new FlexibleTinyIntCodec());
        codecRegistry.register(new FlexibleSmallIntCodec());
        codecRegistry.register(new FlexibleIntCodec());
        codecRegistry.register(new FlexibleBigIntCodec());
        codecRegistry.register(new FlexibleFloatCodec());
        codecRegistry.register(new FlexibleDoubleCodec());
    }
}
