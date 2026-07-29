package org.apache.nifi.testharness.cql;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DefaultDriverOption;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;

import javax.net.ssl.SSLContext;
import java.time.Duration;

/**
 * Bootstraps username/password authentication once a container is up: connects with a bootstrap identity that's
 * already usable (Cassandra's built-in default superuser, or - for ScyllaDB 2026.2+, which no longer provisions
 * one automatically - the custom {@code auth_superuser_name}/{@code auth_superuser_salted_password} identity
 * {@link StartCommand} bakes into the server config), then issues a single {@code CREATE ROLE} for the
 * caller-requested credentials. Mirrors the two-step shape both engines' nifi-cql-bundle integration tests use
 * (see CassandraCQLExecutionServiceConnectionIT#testConnectsWithRealUsernameAndPassword and
 * ScyllaAuthenticationIT#startContainerAndCreateRole).
 */
final class TestUserProvisioner {

    private static final String LOCAL_DATACENTER = "datacenter1";

    // A raw TCP-accept on the published port (StartCommand's initial readiness gate) can succeed well before
    // the database process has actually finished booting and bound its real CQL/TLS listener - observed here
    // taking upwards of a minute after the TCP port was already connectable. This retry budget is the real
    // readiness gate; it needs to comfortably outlast that gap.
    private static final int CONNECT_ATTEMPTS = 40;

    private static final Duration CONNECT_RETRY_DELAY = Duration.ofSeconds(3);

    private TestUserProvisioner() {
    }

    static void createRole(final String contactPoint, final String bootstrapUsername, final String bootstrapPassword,
                            final String targetUsername, final String targetPassword, final SSLContext sslContext) throws InterruptedException {
        // DDL (CREATE ROLE) settles slower than plain reads/writes; the driver's 2-second default request/schema-
        // agreement timeouts aren't always enough immediately after a container reports its port open.
        final DriverConfigLoader longTimeoutConfig = DriverConfigLoader.programmaticBuilder()
                .withDuration(DefaultDriverOption.REQUEST_TIMEOUT, Duration.ofSeconds(20))
                .withDuration(DefaultDriverOption.CONTROL_CONNECTION_AGREEMENT_TIMEOUT, Duration.ofSeconds(20))
                .build();

        RuntimeException lastFailure = null;
        for (int attempt = 1; attempt <= CONNECT_ATTEMPTS; attempt++) {
            final CqlSessionBuilder sessionBuilder = CqlSession.builder()
                    .addContactPoint(parseContactPoint(contactPoint))
                    .withLocalDatacenter(LOCAL_DATACENTER)
                    .withAuthCredentials(bootstrapUsername, bootstrapPassword)
                    .withConfigLoader(longTimeoutConfig);
            if (sslContext != null) {
                sessionBuilder.withSslContext(sslContext);
            }
            try (CqlSession session = sessionBuilder.build()) {
                session.execute(String.format(
                        "CREATE ROLE IF NOT EXISTS %s WITH PASSWORD = '%s' AND LOGIN = true AND SUPERUSER = true",
                        targetUsername, targetPassword.replace("'", "''")));
                return;
            } catch (final RuntimeException e) {
                lastFailure = e;
                if (attempt < CONNECT_ATTEMPTS) {
                    Thread.sleep(CONNECT_RETRY_DELAY.toMillis());
                }
            }
        }
        throw new IllegalStateException("Could not create test user role after " + CONNECT_ATTEMPTS + " attempts", lastFailure);
    }

    private static java.net.InetSocketAddress parseContactPoint(final String contactPoint) {
        final int colon = contactPoint.lastIndexOf(':');
        final String host = contactPoint.substring(0, colon);
        final int port = Integer.parseInt(contactPoint.substring(colon + 1));
        return java.net.InetSocketAddress.createUnresolved(host, port);
    }
}
