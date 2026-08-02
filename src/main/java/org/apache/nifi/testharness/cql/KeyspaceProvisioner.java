package org.apache.nifi.testharness.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import javax.net.ssl.SSLContext;
import java.net.InetSocketAddress;
import java.time.Duration;

/**
 * Creates {@link StartCommand}'s default keyspace once a container is ready, using SimpleStrategy with
 * replication_factor 1 - SimpleStrategy has no true "no replication" setting, so a single unreplicated
 * copy is the closest equivalent. Connects with whatever bootstrap identity is on hand (null/no-auth if
 * --with-test-user wasn't requested, since no authenticator is configured on the container in that case
 * either). Mirrors the connect-with-retry shape TestUserProvisioner uses for the same reason: a
 * container's TCP port can accept connections well before its CQL listener has actually finished booting.
 */
final class KeyspaceProvisioner {

    private static final String LOCAL_DATACENTER = "datacenter1";

    private static final int CONNECT_ATTEMPTS = 40;

    private static final Duration CONNECT_RETRY_DELAY = Duration.ofSeconds(3);

    private KeyspaceProvisioner() {
    }

    static void createKeyspace(final Engine engine, final String contactPoint, final String bootstrapUsername, final String bootstrapPassword,
                                final String keyspaceName, final SSLContext sslContext) throws InterruptedException {
        final DriverConfigLoader longTimeoutConfig = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(20))
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                .build();

        // ScyllaDB defaults new keyspaces to tablet-based replication, which SimpleStrategy cannot use
        // ("SimpleStrategy doesn't support tablet replication") - tablets must be explicitly disabled to
        // get the plain vnode-based SimpleStrategy behavior. Apache Cassandra has no such concept and
        // rejects an unrecognized "tablets" property outright, so this is engine-specific.
        final String tabletsClause = engine == Engine.SCYLLADB ? " AND tablets = {'enabled': false}" : "";

        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= CONNECT_ATTEMPTS; attempt++) {
            final CqlSessionBuilder sessionBuilder = CqlSession.builder()
                    .addContactPoint(parseContactPoint(contactPoint))
                    .withLocalDatacenter(LOCAL_DATACENTER)
                    .withConfigLoader(longTimeoutConfig);
            if (bootstrapUsername != null) {
                sessionBuilder.withAuthCredentials(bootstrapUsername, bootstrapPassword);
            }
            if (sslContext != null) {
                sessionBuilder.withSslContext(sslContext);
            }
            try (CqlSession session = sessionBuilder.build()) {
                session.execute(String.format(
                        "CREATE KEYSPACE IF NOT EXISTS %s WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}%s",
                        keyspaceName, tabletsClause));
                return;
            } catch (final RuntimeException e) {
                lastFailure = e;
                if (attempt < CONNECT_ATTEMPTS) {
                    Thread.sleep(CONNECT_RETRY_DELAY.toMillis());
                }
            }
        }
        throw new IllegalStateException("Could not create keyspace '" + keyspaceName + "' after " + CONNECT_ATTEMPTS + " attempts", lastFailure);
    }

    private static InetSocketAddress parseContactPoint(final String contactPoint) {
        final int colon = contactPoint.lastIndexOf(':');
        final String host = contactPoint.substring(0, colon);
        final int port = Integer.parseInt(contactPoint.substring(colon + 1));
        return InetSocketAddress.createUnresolved(host, port);
    }
}
