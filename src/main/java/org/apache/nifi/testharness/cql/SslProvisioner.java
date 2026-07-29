package org.apache.nifi.testharness.cql;

import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManagerFactory;
import java.io.IOException;
import java.nio.file.Path;
import java.security.GeneralSecurityException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.List;

/**
 * Builds the TLS material for {@code start --with-ssl} and the {@code client_encryption_options} block each
 * engine's config needs to enable it. One-way mode generates a single self-signed server certificate. Two-way
 * mode additionally builds a pseudo certificate authority and uses it to sign both the server certificate and a
 * client certificate, which is delivered to the caller as a password-less PKCS12 file. In both modes, the
 * server's own certificate (not the CA) is what goes into the JKS truststore handed back for NiFi's
 * SSLContextService - trusting the leaf directly is sufficient for JSSE's trust-path check and keeps one-way
 * and two-way consistent with each other.
 *
 * <p>Cassandra's server-side TLS config is Java-keystore-based; ScyllaDB's is PEM-based (see
 * CassandraCQLExecutionServiceSslIT vs. ScyllaSslIT in nifi-cql-bundle) - each engine branch below mounts and
 * references the file format its own {@code client_encryption_options} schema expects.
 */
final class SslProvisioner {

    private static final String CASSANDRA_KEYSTORE_CONTAINER_PATH = "/etc/cassandra/harness-keystore.jks";
    private static final String CASSANDRA_SERVER_TRUSTSTORE_CONTAINER_PATH = "/etc/cassandra/harness-server-truststore.jks";

    private static final String SCYLLA_CERT_CONTAINER_PATH = "/etc/scylla/ssl/server.crt";
    private static final String SCYLLA_KEY_CONTAINER_PATH = "/etc/scylla/ssl/server.key";
    private static final String SCYLLA_TRUSTSTORE_CONTAINER_PATH = "/etc/scylla/ssl/client-truststore.pem";

    private static final String CA_DN = "CN=nifi-cql-test-harness-ca";
    private static final String SERVER_DN_FORMAT = "CN=%s-server";
    private static final String CLIENT_DN = "CN=nifi-cql-test-harness-client";

    record SslSetupResult(CertificateMaterial.Store trustStore, Path clientPkcs12, String blockKey, String blockBody,
                           SSLContext bootstrapSslContext) {
        String applyTo(final String yaml) {
            return YamlPatcher.replaceBlock(yaml, blockKey, blockBody);
        }
    }

    private SslProvisioner() {
    }

    static SslSetupResult provision(final Engine engine, final SslMode sslMode, final Path outputDirectory, final List<String> mountArgs)
            throws GeneralSecurityException, IOException {
        final boolean twoWay = sslMode == SslMode.TWO_WAY;
        final String serverDn = String.format(SERVER_DN_FORMAT, engine.name().toLowerCase());

        final CertificateMaterial.KeyAndCertificate ca = twoWay ? CertificateMaterial.selfSignedCertificateAuthority(CA_DN) : null;
        final CertificateMaterial.KeyAndCertificate server = twoWay
                ? CertificateMaterial.caSigned(ca, serverDn)
                : CertificateMaterial.selfSigned(serverDn);

        final CertificateMaterial.Store deliveredTrustStore = CertificateMaterial.writeJksTrustStore(
                server.certificate(), "server", outputDirectory.resolve("nifi-truststore.jks"));

        Path clientPkcs12 = null;
        CertificateMaterial.KeyAndCertificate client = null;
        if (twoWay) {
            client = CertificateMaterial.caSigned(ca, CLIENT_DN);
            clientPkcs12 = CertificateMaterial.writePkcs12NoPassword(
                    client.keyPair().getPrivate(), client.certificate(), "client", outputDirectory.resolve("nifi-client-identity.p12"));
        }

        final String blockBody = engine == Engine.CASSANDRA
                ? cassandraBlock(server, ca, twoWay, outputDirectory, mountArgs)
                : scyllaBlock(server, ca, twoWay, outputDirectory, mountArgs);

        // The bootstrap CREATE ROLE connection (TestUserProvisioner) has to speak TLS to the same server, and
        // - in two-way mode - present a certificate the server's own truststore will accept, since
        // require_client_auth applies to every connection, not just the ones NiFi makes later.
        final SSLContext bootstrapSslContext = buildBootstrapSslContext(server.certificate(), client);

        return new SslSetupResult(deliveredTrustStore, clientPkcs12, "client_encryption_options", blockBody, bootstrapSslContext);
    }

    private static SSLContext buildBootstrapSslContext(final X509Certificate serverCertificate, final CertificateMaterial.KeyAndCertificate client)
            throws GeneralSecurityException, IOException {
        final KeyStore trustKeyStore = KeyStore.getInstance("JKS");
        trustKeyStore.load(null);
        trustKeyStore.setCertificateEntry("server", serverCertificate);
        final TrustManagerFactory trustManagerFactory = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
        trustManagerFactory.init(trustKeyStore);

        final KeyManager[] keyManagers;
        if (client == null) {
            keyManagers = null;
        } else {
            final char[] password = "bootstrap".toCharArray();
            final KeyStore clientKeyStore = KeyStore.getInstance("PKCS12");
            clientKeyStore.load(null);
            clientKeyStore.setKeyEntry("client", client.keyPair().getPrivate(), password, new X509Certificate[]{client.certificate()});
            final KeyManagerFactory keyManagerFactory = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());
            keyManagerFactory.init(clientKeyStore, password);
            keyManagers = keyManagerFactory.getKeyManagers();
        }

        final SSLContext sslContext = SSLContext.getInstance("TLS");
        sslContext.init(keyManagers, trustManagerFactory.getTrustManagers(), null);
        return sslContext;
    }

    private static String cassandraBlock(final CertificateMaterial.KeyAndCertificate server, final CertificateMaterial.KeyAndCertificate ca,
                                          final boolean requireClientAuth, final Path outputDirectory, final List<String> mountArgs)
            throws GeneralSecurityException, IOException {
        final CertificateMaterial.Store serverKeyStore = CertificateMaterial.writeJksKeyStore(
                server.keyPair().getPrivate(), server.certificate(), "server", outputDirectory.resolve("server-keystore.jks"));
        addMount(mountArgs, serverKeyStore.path(), CASSANDRA_KEYSTORE_CONTAINER_PATH);

        final StringBuilder block = new StringBuilder()
                .append("  enabled: true\n")
                .append("  optional: false\n")
                .append("  keystore: ").append(CASSANDRA_KEYSTORE_CONTAINER_PATH).append('\n')
                .append("  keystore_password: ").append(serverKeyStore.password()).append('\n')
                .append("  store_type: JKS\n")
                .append("  require_client_auth: ").append(requireClientAuth).append('\n');

        if (requireClientAuth) {
            final CertificateMaterial.Store serverTrustStore = CertificateMaterial.writeJksTrustStore(
                    ca.certificate(), "ca", outputDirectory.resolve("server-truststore.jks"));
            addMount(mountArgs, serverTrustStore.path(), CASSANDRA_SERVER_TRUSTSTORE_CONTAINER_PATH);
            block.append("  truststore: ").append(CASSANDRA_SERVER_TRUSTSTORE_CONTAINER_PATH).append('\n')
                    .append("  truststore_password: ").append(serverTrustStore.password()).append('\n');
        }
        return block.toString();
    }

    private static String scyllaBlock(final CertificateMaterial.KeyAndCertificate server, final CertificateMaterial.KeyAndCertificate ca,
                                       final boolean requireClientAuth, final Path outputDirectory, final List<String> mountArgs)
            throws GeneralSecurityException, IOException {
        final Path serverCertPem = CertificateMaterial.writeCertificatePem(server.certificate(), outputDirectory.resolve("server-cert.pem"));
        final Path serverKeyPem = CertificateMaterial.writePrivateKeyPem(server.keyPair().getPrivate(), outputDirectory.resolve("server-key.pem"));
        addMount(mountArgs, serverCertPem, SCYLLA_CERT_CONTAINER_PATH);
        addMount(mountArgs, serverKeyPem, SCYLLA_KEY_CONTAINER_PATH);

        final StringBuilder block = new StringBuilder()
                .append("  enabled: true\n")
                .append("  certificate: ").append(SCYLLA_CERT_CONTAINER_PATH).append('\n')
                .append("  keyfile: ").append(SCYLLA_KEY_CONTAINER_PATH).append('\n')
                .append("  require_client_auth: ").append(requireClientAuth).append('\n');

        if (requireClientAuth) {
            final X509Certificate caCertificate = ca.certificate();
            final Path caCertPem = CertificateMaterial.writeCertificatePem(caCertificate, outputDirectory.resolve("client-truststore.pem"));
            addMount(mountArgs, caCertPem, SCYLLA_TRUSTSTORE_CONTAINER_PATH);
            block.append("  truststore: ").append(SCYLLA_TRUSTSTORE_CONTAINER_PATH).append('\n');
        }
        return block.toString();
    }

    private static void addMount(final List<String> mountArgs, final Path hostPath, final String containerPath) {
        mountArgs.add("-v");
        // Not mounted read-only: Cassandra's and ScyllaDB's entrypoint scripts chown files under /etc on
        // startup (to match the container's runtime user), which fails and aborts the container if the mount
        // is read-only.
        mountArgs.add(hostPath.toAbsolutePath() + ":" + containerPath);
    }
}
